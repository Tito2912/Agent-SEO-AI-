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
import logging
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

logger = logging.getLogger("seo_agent")
from collections import Counter, deque
from contextlib import asynccontextmanager, contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from email.utils import formataddr
from functools import lru_cache
from pathlib import Path
from collections.abc import Callable
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
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
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
    from . import repo_index as repo_index  # type: ignore
except ImportError:
    # When running from inside this folder (`uvicorn app:app`) or with `--app-dir seo-agent-web/backend`.
    import repo_index  # type: ignore

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
    # When running as `uvicorn backend.app:app` (recommended).
    from . import content_library as content_library  # type: ignore
except ImportError:
    # When running from inside this folder (`uvicorn app:app`) or with `--app-dir seo-agent-web/backend`.
    import content_library  # type: ignore


try:
    from .db import Database  # type: ignore
    from .models import (  # type: ignore
        AuditLog,
        BacklinkOpportunity,
        BillingSubscription,
        CompetitorSite,
        EmailVerificationToken,
        IssueTask,
        JobRecord,
        OAuthIdentity,
        PasswordResetToken,
        Project,
        RateLimitBucket,
        TrackedKeyword,
        User,
        UserConnection,
    )
    from . import auth as auth  # type: ignore
    from . import keywords as keywords_mod  # type: ignore
    from . import competitors as competitors_mod  # type: ignore
except ImportError:
    from db import Database  # type: ignore
    from models import (  # type: ignore
        AuditLog,
        BacklinkOpportunity,
        BillingSubscription,
        CompetitorSite,
        EmailVerificationToken,
        IssueTask,
        JobRecord,
        OAuthIdentity,
        PasswordResetToken,
        Project,
        RateLimitBucket,
        TrackedKeyword,
        User,
        UserConnection,
    )
    import auth as auth  # type: ignore
    import keywords as keywords_mod  # type: ignore
    import competitors as competitors_mod  # type: ignore


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
_CSRF_EXEMPT_PATHS = {"/healthz", "/stripe/webhook", "/cron/check-backlinks", "/cron/autopilot", "/cron/auto-search-backlinks", "/cron/auto-post-backlinks", "/cron/refresh-competitors"}

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


def _ensure_runs_file_local(path: Path) -> bool:
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
    return bool(object_store.restore_runs_file(root, target))


def _sync_runs_path_to_object_store(path: Path) -> None:
    try:
        object_store.upload_runs_path(DEFAULT_RUNS_DIR, path.resolve())
    except Exception as e:
        logger.error("[S3] upload error for %s: %s: %s", path, type(e).__name__, e)


def _delete_runs_path_from_object_store(path: Path, *, recursive: bool = False) -> None:
    try:
        object_store.delete_runs_path(DEFAULT_RUNS_DIR, path.resolve(), recursive=recursive)
    except Exception as e:
        logger.error("[S3] delete error for %s: %s: %s", path, type(e).__name__, e)


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
    """Env value with a MATCHED pair of surrounding quotes removed, and nothing else.

    `.strip('"').strip("'")` removes every quote character at either end, not a wrapping pair.
    A secret ending in a quote came back truncated — silently, and every value in this app
    passes through here, including API keys and SMTP passwords. Found by a CSP policy ending
    in `'self'` losing its last character.
    """
    raw = str(os.environ.get(name) or "").strip()
    for quote_char in ('"', "'"):
        if len(raw) >= 2 and raw[0] == quote_char and raw[-1] == quote_char:
            return raw[1:-1]
    return raw


def _env_bool(name: str) -> bool:
    v = _safe_env(name).lower()
    return v in {"1", "true", "yes", "y", "on"}


def _env_bool_default(name: str, default: bool) -> bool:
    raw = _safe_env(name).lower()
    if not raw:
        return bool(default)
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _env_list(name: str) -> list[str]:
    raw = _safe_env(name)
    if not raw:
        return []
    parts = [p.strip() for p in re.split(r"[,\n;]+", raw) if p and p.strip()]
    return [p for p in parts if p]


def _prod_like_environment() -> bool:
    env_names = [
        _safe_env("SENTRY_ENVIRONMENT"),
        _safe_env("SEO_AGENT_ENV"),
        _safe_env("APP_ENV"),
        _safe_env("ENVIRONMENT"),
    ]
    if any(v.lower() in {"prod", "production"} for v in env_names if v):
        return True
    return bool(_safe_env("RENDER") or _safe_env("RENDER_SERVICE_NAME"))


def _strict_config_enabled() -> bool:
    return _env_bool_default("SEO_AGENT_STRICT_CONFIG", _prod_like_environment())


def _trust_proxy_headers() -> bool:
    return _env_bool_default("SEO_AGENT_TRUST_PROXY_HEADERS", _prod_like_environment())


def _csp_enabled() -> bool:
    return _env_bool_default("SEO_AGENT_CSP_ENABLED", True)


def _csp_report_only() -> bool:
    return _env_bool("SEO_AGENT_CSP_REPORT_ONLY")


def _content_security_policy(request: Request) -> str:
    custom = _safe_env("SEO_AGENT_CSP")
    if custom:
        return re.sub(r"\s+", " ", custom).strip()

    directives = [
        "default-src 'self'",
        "base-uri 'self'",
        "object-src 'none'",
        "frame-ancestors 'none'",
        # Chrome and Safari enforce form-action across the REDIRECT that follows a form POST,
        # so `'self'` alone silently blocks every hand-off this app makes: Stripe checkout and
        # the billing portal, and "Continuer avec Google". The server logs a successful 303 and
        # the browser does nothing — the failure has no server-side trace at all.
        # Only hosts this app actually 303s a form POST to are listed.
        "form-action 'self' "
        "https://checkout.stripe.com https://billing.stripe.com "
        "https://accounts.google.com https://github.com "
        "https://app.netlify.com https://www.bing.com",
        "img-src 'self' data: https:",
        "font-src 'self' data:",
        "style-src 'self' 'unsafe-inline'",
        "script-src 'self' 'unsafe-inline'",
        "connect-src 'self'",
        "frame-src 'none'",
        "manifest-src 'self'",
    ]
    if _request_is_secure(request):
        directives.append("upgrade-insecure-requests")
    return "; ".join(directives)


def _weak_secret(value: str) -> bool:
    raw = str(value or "").strip()
    low = raw.lower()
    if len(raw) < 32:
        return True
    weak_exact = {
        "change_me",
        "changeme",
        "replace_me",
        "replace_me_with_long_random_secret",
        "test-secret",
        "test-session-secret",
        "test-encryption-secret",
    }
    return low in weak_exact or low.startswith(("change_me", "replace_me", "test-"))


def _current_encryption_seed() -> str:
    raw_keys = _safe_env("SEO_AGENT_ENCRYPTION_KEYS")
    if raw_keys:
        for part in re.split(r"[,\n;]+", raw_keys):
            seed = part.strip()
            if seed:
                return seed
    return _safe_env("SEO_AGENT_ENCRYPTION_KEY")

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


def _secret_encryption_source() -> str:
    if _safe_env("SEO_AGENT_ENCRYPTION_KEYS").strip():
        return "SEO_AGENT_ENCRYPTION_KEYS"
    if _safe_env("SEO_AGENT_ENCRYPTION_KEY").strip():
        return "SEO_AGENT_ENCRYPTION_KEY"
    if _safe_env("SEO_AGENT_SECRET_KEY").strip():
        return "SEO_AGENT_SECRET_KEY"
    return ""


def _require_secret_encryption_ready() -> None:
    if Fernet is None:
        raise RuntimeError("cryptography_missing")
    if not _encryption_seeds():
        raise RuntimeError("secret_encryption_key_missing")
    if not _fernets():
        raise RuntimeError("invalid_secret_encryption_key")


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
    return not bool(meta.get("hide_from_connections"))

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
    _require_secret_encryption_ready()
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


def _secret_storage_health() -> dict[str, Any]:
    seeds = _encryption_seeds()
    fernet_count = len(_fernets()) if Fernet is not None else 0
    stats: dict[str, Any] = {
        "configured": bool(seeds) and Fernet is not None and fernet_count > 0,
        "source": _secret_encryption_source(),
        "seed_count": len(seeds),
        "fernet_count": fernet_count,
        "total": 0,
        "encrypted_current": 0,
        "encrypted_legacy": 0,
        "plaintext": 0,
        "empty": 0,
        "unreadable": 0,
        "legacy_gsc_files": 0,
    }
    with DB.session() as db:
        rows = list(db.scalars(select(UserConnection)))
    for row in rows:
        stats["total"] += 1
        stored = str(getattr(row, "secret_value", "") or "").strip()
        if not stored:
            stats["empty"] += 1
            continue
        if not stored.startswith(_SECRET_PREFIX):
            stats["plaintext"] += 1
            continue
        try:
            value, rotated = _decrypt_secret_with_rotation(stored)
            if not value:
                stats["unreadable"] += 1
            elif rotated:
                stats["encrypted_legacy"] += 1
            else:
                stats["encrypted_current"] += 1
        except Exception:
            stats["unreadable"] += 1
    try:
        for path in GSC_OAUTH_DIR.rglob("*.json"):
            rel = path.relative_to(GSC_OAUTH_DIR)
            if rel.parts and rel.parts[0] == "_runtime":
                continue
            stats["legacy_gsc_files"] += 1
    except Exception:
        pass
    return stats


def _rotate_user_connection_secrets() -> dict[str, int]:
    _require_secret_encryption_ready()
    counts = {
        "total": 0,
        "rotated": 0,
        "unchanged": 0,
        "plaintext_reencrypted": 0,
        "unreadable": 0,
        "empty": 0,
    }
    with DB.session() as db:
        rows = list(db.scalars(select(UserConnection)))
        for row in rows:
            counts["total"] += 1
            stored = str(getattr(row, "secret_value", "") or "").strip()
            if not stored:
                counts["empty"] += 1
                continue
            if not stored.startswith(_SECRET_PREFIX):
                row.secret_value = _encrypt_secret(stored)
                db.add(row)
                counts["plaintext_reencrypted"] += 1
                counts["rotated"] += 1
                continue
            try:
                value, rotated = _decrypt_secret_with_rotation(stored)
            except Exception:
                counts["unreadable"] += 1
                continue
            if not value:
                counts["unreadable"] += 1
                continue
            if rotated:
                row.secret_value = _encrypt_secret(value)
                db.add(row)
                counts["rotated"] += 1
            else:
                counts["unchanged"] += 1
        db.commit()
    return counts


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
                    try:
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
                                "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds") + "Z",
                            }
                            oauth_row.meta = oauth_meta
                            session.add(oauth_row)
                            session.commit()
                    except Exception as _bing_refresh_err:
                        logger.warning(
                            "[Bing] refresh token failed for user=%s: %s: %s",
                            user_id, type(_bing_refresh_err).__name__, _bing_refresh_err,
                        )
                        access_token = ""
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

    proto = "https" if _request_is_secure(request) else (request.url.scheme or "http")
    forwarded_host = request.headers.get("x-forwarded-host") if _trust_proxy_headers() else ""
    host = (forwarded_host or request.headers.get("host") or request.url.netloc or "").split(",")[0].strip()
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


def _safe_storage_segment(value: str, default: str) -> str:
    return re.sub(r"[^a-z0-9_.-]+", "-", (value or "").strip().lower()).strip("-") or default


def _gsc_oauth_connection_key(slug: str) -> str:
    safe_slug = _safe_storage_segment(slug, "project")
    prefix = "GSC_OAUTH:"
    if len(safe_slug) <= 80:
        return f"{prefix}{safe_slug}"
    digest = hashlib.sha256(safe_slug.encode("utf-8")).hexdigest()[:8]
    return f"{prefix}{safe_slug[:80]}:{digest}"


def _gsc_oauth_user_dir(user_id: str, *, create: bool = True) -> Path:
    user_dir = (GSC_OAUTH_DIR / _safe_storage_segment(user_id, "user")).resolve()
    if create:
        user_dir.mkdir(parents=True, exist_ok=True)
    return user_dir


def _gsc_oauth_token_path(user_id: str, slug: str) -> Path:
    safe_slug = _safe_storage_segment(slug, "project")
    return _gsc_oauth_user_dir(user_id, create=True) / f"{safe_slug}.json"


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
        except Exception as _e:
            logger.warning("[GSC] OAuth token migration save failed: %s: %s", type(_e).__name__, _e)
    return value


def _gsc_oauth_connected(user_id: str, slug: str) -> bool:
    return bool(_gsc_oauth_refresh_token(user_id, slug))


def _gsc_oauth_save(user_id: str, slug: str, *, refresh_token: str, scope: str) -> None:
    meta = {
        "v": 1,
        "type": "google_oauth_refresh_token",
        "scope": str(scope or "").strip() or _GOOGLE_OAUTH_SCOPE,
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds") + "Z",
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


def _gsc_runtime_oauth_dir(user_id: str, *, create: bool = True) -> Path:
    runtime_dir = (GSC_OAUTH_DIR / "_runtime" / _safe_storage_segment(user_id, "user")).resolve()
    if create:
        runtime_dir.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(runtime_dir, 0o700)
        except Exception:
            pass
    return runtime_dir


def _gsc_write_runtime_oauth_credentials(*, user_id: str, slug: str, refresh_token: str) -> Path:
    """
    Create a short-lived OAuth "authorized_user" JSON file for gsc_fetch / seo_audit.

    We keep refresh tokens encrypted in DB; this file exists only for the duration of a single request/job.
    """
    safe_slug = re.sub(r"[^a-z0-9_.-]+", "-", (slug or "").strip().lower()).strip("-") or "project"
    runtime_dir = _gsc_runtime_oauth_dir(user_id)
    path = (runtime_dir / f"{safe_slug}-{uuid.uuid4().hex}.json").resolve()
    payload = {
        "type": "authorized_user",
        "refresh_token": str(refresh_token or "").strip(),
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    try:
        os.chmod(path, 0o600)
    except Exception:
        pass
    return path


@contextmanager
def _gsc_live_credentials(*, user_id: str, slug: str) -> Any:
    """
    Yield a credentials path usable by `gsc_fetch`:
    - Prefer per-project OAuth refresh token stored in DB (temp file)
    - Fall back to service account credentials (GOOGLE_APPLICATION_CREDENTIALS)
    """
    oauth_refresh = _gsc_oauth_refresh_token(user_id, slug)
    if oauth_refresh:
        client_id, client_secret = _google_oauth_client()
        if not client_id or not client_secret:
            yield None, "oauth", "oauth_not_configured"
            return

        runtime_path: Path | None = None
        try:
            runtime_path = _gsc_write_runtime_oauth_credentials(user_id=user_id, slug=slug, refresh_token=oauth_refresh)
            yield runtime_path, "oauth", ""
        finally:
            if runtime_path is not None:
                try:
                    runtime_path.unlink()
                except Exception:
                    pass
        return

    env_creds = _resolve_repo_path(_safe_env("GOOGLE_APPLICATION_CREDENTIALS"))
    if env_creds and env_creds.exists():
        yield env_creds, "service_account", ""
        return

    yield None, "", "missing_credentials"


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


_GITHUB_REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_GITHUB_BRANCH_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,199}$")
_GITHUB_MAX_FILE_PATH_LEN = 500
_GITHUB_MAX_PATCHED_CONTENT_BYTES = 250_000


def _github_repo_parts(repo: str) -> tuple[str, str] | None:
    candidate = (repo or "").strip()
    if not _GITHUB_REPO_RE.fullmatch(candidate):
        return None
    owner, repo_name = candidate.split("/", 1)
    if owner in {".", ".."} or repo_name in {".", ".."}:
        return None
    return owner, repo_name


def _github_branch_allowed(branch: str) -> bool:
    candidate = (branch or "").strip()
    if not _GITHUB_BRANCH_RE.fullmatch(candidate):
        return False
    if candidate.startswith("/") or candidate.endswith("/") or candidate.endswith("."):
        return False
    if ".." in candidate or "//" in candidate or "@{" in candidate or "\\" in candidate:
        return False
    return all(part not in {"", ".", ".."} and not part.endswith(".lock") for part in candidate.split("/"))


def _github_file_path_allowed(path: str) -> bool:
    candidate = (path or "").strip()
    if not candidate or len(candidate) > _GITHUB_MAX_FILE_PATH_LEN:
        return False
    if candidate.startswith("/") or "\\" in candidate or _has_control_chars(candidate):
        return False
    parts = candidate.split("/")
    return all(part not in {"", ".", ".."} for part in parts)


def _github_patched_content_error(content: str) -> str | None:
    if not content:
        return "Contenu patché manquant."
    if len(content.encode("utf-8")) > _GITHUB_MAX_PATCHED_CONTENT_BYTES:
        return f"Contenu patché trop volumineux ({_GITHUB_MAX_PATCHED_CONTENT_BYTES // 1000} kB max)."
    return None


def _safe_github_branch_suffix(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", (value or "").strip()).strip(".-")
    return (slug[:60] or "issue").rstrip(".")


def _github_api_path(*parts: str) -> str:
    cleaned: list[str] = []
    for part in parts:
        value = str(part)
        if not value or _has_control_chars(value):
            raise ValueError("Invalid GitHub API path segment")
        cleaned.append(quote(value, safe=""))
    return "/" + "/".join(cleaned)


def _github_content_api_path(owner: str, repo: str, path: str) -> str:
    if not _github_file_path_allowed(path):
        raise ValueError("Invalid GitHub file path")
    return _github_api_path("repos", owner, repo, "contents", *path.split("/"))


def _github_ref_api_path(owner: str, repo: str, branch: str) -> str:
    if not _github_branch_allowed(branch):
        raise ValueError("Invalid GitHub branch")
    return _github_api_path("repos", owner, repo, "git", "ref", "heads", *branch.split("/"))


def _github_api_url(path: str) -> str:
    if not path.startswith("/") or path.startswith("//") or _has_control_chars(path):
        raise RuntimeError("Invalid GitHub API path")
    return f"https://api.github.com{path}"


def _github_api_get(path: str, *, token: str, params: dict[str, Any] | None = None, timeout_s: float = 30.0) -> Any:
    resp = requests.get(
        _github_api_url(path),
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


def _github_api_post(path: str, *, token: str, json_body: dict[str, Any], timeout_s: float = 30.0) -> Any:
    resp = requests.post(
        _github_api_url(path),
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "seo-agent-web",
        },
        json=json_body,
        timeout=timeout_s,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"GitHub {resp.status_code}: {(resp.text or '').strip()[:400]}")
    try:
        return resp.json()
    except Exception as e:
        raise RuntimeError(f"GitHub JSON decode error: {e}") from e


def _github_api_put(path: str, *, token: str, json_body: dict[str, Any], timeout_s: float = 30.0) -> Any:
    resp = requests.put(
        _github_api_url(path),
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "seo-agent-web",
        },
        json=json_body,
        timeout=timeout_s,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"GitHub {resp.status_code}: {(resp.text or '').strip()[:400]}")
    try:
        return resp.json()
    except Exception as e:
        raise RuntimeError(f"GitHub JSON decode error: {e}") from e


def _github_api_delete(path: str, *, token: str, json_body: dict[str, Any], timeout_s: float = 30.0) -> Any:
    resp = requests.delete(
        _github_api_url(path),
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "seo-agent-web",
        },
        json=json_body,
        timeout=timeout_s,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"GitHub {resp.status_code}: {(resp.text or '').strip()[:400]}")
    try:
        return resp.json()
    except Exception as e:
        raise RuntimeError(f"GitHub JSON decode error: {e}") from e


def _github_pr_merged(owner: str, repo: str, pr_number: int, token: str) -> bool:
    """Return True if the given pull request is closed or merged (i.e. no longer open).
    Best-effort: any error (network, missing token, rate limit) is treated as 'not done'
    so the UI degrades gracefully."""
    if not token or not owner or not repo or pr_number <= 0:
        return False
    try:
        data = _github_api_get(
            _github_api_path("repos", owner, repo, "pulls", str(int(pr_number))),
            token=token,
            timeout_s=8,
        )
    except Exception:
        return False
    if not isinstance(data, dict):
        return False
    # Hide "PR existante" when the PR is no longer open (merged OR closed/rejected)
    return data.get("merged") or bool(data.get("merged_at")) or data.get("state") == "closed"


def _github_pr_is_open(owner: str, repo: str, pr_number: int, token: str) -> bool:
    """True only when GitHub CONFIRMS the pull request is still open.

    Deliberately the inverse of `_github_pr_merged`'s error handling: this one gates an action,
    so anything unknown (network, rate limit, missing token, deleted PR) must return False and
    let the user through rather than block them on a guess."""
    if not token or not owner or not repo or pr_number <= 0:
        return False
    try:
        data = _github_api_get(
            _github_api_path("repos", owner, repo, "pulls", str(int(pr_number))),
            token=token,
            timeout_s=8,
        )
    except Exception:
        return False
    if not isinstance(data, dict):
        return False
    return data.get("state") == "open" and not data.get("merged") and not data.get("merged_at")


def _netlify_api_url(path: str) -> str:
    if not path.startswith("/") or path.startswith("//") or _has_control_chars(path):
        raise RuntimeError("Invalid Netlify API path")
    return f"https://api.netlify.com{path}"


def _netlify_api_get(path: str, *, token: str, params: dict[str, Any] | None = None, timeout_s: float = 30.0) -> Any:
    resp = requests.get(
        _netlify_api_url(path),
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
        _netlify_api_url(path),
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
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
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
    created_at = datetime.now(timezone.utc)
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
        "connected_at": datetime.now(timezone.utc).isoformat(timespec="seconds") + "Z",
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


_CONFIG_FILE_SUFFIXES = {".json", ".yaml", ".yml"}


def _resolve_config_path(raw: str | Path | None) -> Path:
    value = str(raw or "").strip()
    p = Path(value).expanduser() if value else DEFAULT_CONFIG
    if not p.is_absolute():
        p = (REPO_ROOT / p).resolve()
    else:
        p = p.resolve()
    return p


def _allowed_config_roots() -> list[Path]:
    roots: list[Path] = []
    for root in [REPO_ROOT, DEFAULT_CONFIG.parent, DATA_DIR]:
        resolved = root.resolve()
        if resolved not in roots:
            roots.append(resolved)
    return roots


def _config_path_allowed(path: Path) -> bool:
    p = path.resolve()
    if p.suffix.lower() not in _CONFIG_FILE_SUFFIXES:
        return False
    return any(p.is_relative_to(root) for root in _allowed_config_roots())


def _resolve_request_config_path(request: Request, raw: str | Path | None) -> Path:
    p = _resolve_config_path(raw)
    user = getattr(request.state, "user", None)
    is_admin = bool(getattr(user, "is_admin", False))
    if not is_admin and p != DEFAULT_CONFIG.resolve():
        raise HTTPException(status_code=403, detail="Config path not allowed")
    if not _config_path_allowed(p):
        raise HTTPException(status_code=403, detail="Config path not allowed")
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


def _file_view_max_bytes() -> int:
    raw = _safe_env("SEO_AGENT_FILE_VIEW_MAX_BYTES")
    try:
        value = int(raw) if raw else 2 * 1024 * 1024
    except Exception:
        value = 2 * 1024 * 1024
    return max(64 * 1024, min(value, 20 * 1024 * 1024))


def _csrf_body_max_bytes() -> int:
    raw = _safe_env("SEO_AGENT_CSRF_BODY_MAX_BYTES")
    try:
        value = int(raw) if raw else 12 * 1024 * 1024
    except Exception:
        value = 12 * 1024 * 1024
    return max(64 * 1024, min(value, 50 * 1024 * 1024))


def _csv_safe_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value
    text = str(value)
    if text and text[0] in {"=", "+", "-", "@", "\t", "\r"}:
        return "'" + text
    return text


def _csv_bytes(rows: list[dict[str, Any]], *, fieldnames: list[str]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({field: _csv_safe_value(row.get(field)) for field in fieldnames})
    return b"\xef\xbb\xbf" + buf.getvalue().encode("utf-8")


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
            if s.startswith(("http://", "https://")) and " " not in s and "->" not in s:
                url = s
            elif "->" in s:
                left = s.split("->", 1)[0].strip()
                if left.startswith(("http://", "https://")):
                    url = left
            if not url:
                m = re.search(r"https?://\S+", s)
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
    # Works with either correction provider (Claude preferred, OpenAI fallback).
    has_ai = bool(
        (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
        or (os.environ.get("OPENAI_API_KEY") or "").strip()
    )
    return enabled and has_ai


def _assistant_openai_configured() -> bool:
    return bool((os.environ.get("OPENAI_API_KEY") or "").strip())


def _assistant_gemini_configured() -> bool:
    return bool((os.environ.get("GOOGLE_GEMINI_API_KEY") or "").strip())


def _assistant_claude_configured() -> bool:
    return bool((os.environ.get("ANTHROPIC_API_KEY") or "").strip())


def _assistant_effective_provider() -> str:
    raw = (os.environ.get("SEO_AUDIT_ASSISTANT_PROVIDER") or "auto").strip().lower()
    if raw in {"openai", "gemini", "claude"}:
        return raw
    # auto: prefer claude > openai > gemini
    if _assistant_claude_configured():
        return "claude"
    if _assistant_openai_configured():
        return "openai"
    if _assistant_gemini_configured():
        return "gemini"
    return "none"


def _assistant_model(provider: str) -> str:
    provider = (provider or "").strip().lower()
    if provider == "claude":
        return (os.environ.get("SEO_AUDIT_ASSISTANT_CLAUDE_MODEL") or "claude-haiku-4-5-20251001").strip()
    if provider == "gemini":
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


def _assistant_claude_chat(messages: list[dict[str, str]], *, system: str, model: str) -> str:
    api_key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY manquante")

    payload: dict[str, Any] = {
        "model": model,
        "max_tokens": 1024,
        "temperature": 0.3,
        "system": system,
        "messages": messages,
    }

    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json=payload,
        timeout=90,
    )
    if resp.status_code != 200:
        err_msg = None
        try:
            err_data = resp.json()
            if isinstance(err_data, dict):
                e = err_data.get("error")
                if isinstance(e, dict):
                    err_msg = str(e.get("message") or "").strip()
        except Exception:
            pass
        raise RuntimeError(f"Claude HTTP {resp.status_code}" + (f": {err_msg}" if err_msg else ""))

    data = resp.json()
    if isinstance(data, dict):
        content_blocks = data.get("content")
        if isinstance(content_blocks, list) and content_blocks:
            first = content_blocks[0]
            if isinstance(first, dict) and first.get("type") == "text":
                text = first.get("text")
                if isinstance(text, str) and text.strip():
                    return text.strip()
    raise RuntimeError("Réponse Claude vide")


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
            "generated_at": datetime.now(timezone.utc).isoformat() + "Z",
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
        _ensure_runs_file_local(path)
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
        _ensure_runs_file_local(path)
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
    parsed = _correction_ai_json(
        system=system,
        user_msg=json.dumps(user, ensure_ascii=False),
        max_tokens=4000,
        temperature=0.2,
    )
    issues_out = parsed.get("issues") if isinstance(parsed, dict) else None
    return issues_out if isinstance(issues_out, dict) else {}


# ---------------------------------------------------------------------------
# Unified correction-AI layer (Claude preferred, OpenAI fallback)
# ---------------------------------------------------------------------------
def _correction_ai_provider() -> str:
    """Provider used to GENERATE code corrections (distinct from the chat assistant).

    Order: explicit env override > Claude (most capable) > OpenAI > none.
    Override with SEO_CORRECTION_AI_PROVIDER = anthropic | openai | none.
    """
    raw = (os.environ.get("SEO_CORRECTION_AI_PROVIDER") or "auto").strip().lower()
    if raw in {"anthropic", "claude"}:
        return "anthropic"
    if raw == "openai":
        return "openai"
    if raw == "none":
        return "none"
    if (os.environ.get("ANTHROPIC_API_KEY") or "").strip():
        return "anthropic"
    if (os.environ.get("OPENAI_API_KEY") or "").strip():
        return "openai"
    return "none"


def _correction_ai_model(provider: str) -> str:
    provider = (provider or "").strip().lower()
    if provider == "anthropic":
        return (os.environ.get("SEO_CORRECTION_ANTHROPIC_MODEL") or "claude-opus-4-8").strip()
    if provider == "openai":
        # gpt-4o-mini is broadly available; override via OPENAI_CHAT_MODEL if your
        # account has a newer model (e.g. gpt-5.1-mini).
        return (
            os.environ.get("OPENAI_CHAT_MODEL")
            or os.environ.get("OPENAI_MODEL")
            or "gpt-4o-mini"
        ).strip()
    return ""


def _plan_correction_cfg(user: Any) -> dict[str, Any]:
    """Resolve the correction engine config (model + max_files/PR) from the user's plan.

    Numbers come from billing.plan_catalog (admin-overridable via PLAN_CONFIG_JSON). Admins
    are unlimited. Free plans have max_files 0 / quota 0 (corrections not included)."""
    if bool(getattr(user, "is_admin", False)):
        model = (os.environ.get("SEO_CORRECTION_ANTHROPIC_MODEL") or "claude-opus-4-8").strip()
        return {"plan": "admin", "model": model, "max_files": 40, "unlimited": True}
    plan = "free"
    try:
        with DB.session() as _db:
            plan = billing.effective_plan_key(_db, user_id=str(getattr(user, "id", "") or ""))
    except Exception:
        plan = "free"
    base = billing.correction_config_for_plan(plan)
    return {"plan": plan, "model": str(base["model"]), "max_files": int(base["max_files"]), "unlimited": False}


def _correction_gate(user: Any) -> tuple[bool, str, int, str]:
    """Check whether the user may run an AI correction now.

    Returns (allowed, error_message, effective_max_files, model_override).
    Admins bypass quota. Caps effective_max_files to the remaining monthly quota."""
    cfg = _plan_correction_cfg(user)
    if cfg["unlimited"]:
        return True, "", int(cfg["max_files"]), str(cfg["model"])
    if int(cfg["max_files"]) <= 0:
        return False, "Les corrections IA ne sont pas incluses dans ton forfait. Passe à un plan supérieur.", 0, ""
    try:
        with DB.session() as _db:
            remaining = billing.remaining_quota(_db, user_id=str(getattr(user, "id", "") or ""), metric="ai_corrections_month")
    except Exception:
        remaining = None
    if isinstance(remaining, int) and remaining <= 0:
        return False, "Quota de corrections IA atteint ce mois-ci. Va sur Abonnement pour upgrade.", 0, ""
    cap = int(cfg["max_files"])
    if isinstance(remaining, int):
        cap = max(1, min(cap, remaining))
    return True, "", cap, str(cfg["model"])


def _correction_charge(user: Any, count: int) -> None:
    """Bill `count` AI corrections (files patched / previews) against the monthly quota. No-op for admins."""
    if count <= 0 or bool(getattr(user, "is_admin", False)):
        return
    try:
        with DB.session() as _db:
            billing.usage_add(_db, user_id=str(getattr(user, "id", "") or ""), metric="ai_corrections_month", amount=int(count))
    except Exception:
        pass


def _parse_ai_json(text: str) -> dict[str, Any]:
    """Tolerant JSON-object extraction (handles ```json fences / surrounding prose)."""
    if not isinstance(text, str):
        return {}
    s = text.strip()
    if not s:
        return {}
    try:
        parsed = json.loads(s)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        pass
    # Strip markdown code fences then retry on the largest {...} span.
    if s.startswith("```"):
        s = s.split("```", 2)[1] if s.count("```") >= 2 else s
        if s.lower().startswith("json"):
            s = s[4:]
        s = s.strip()
        try:
            parsed = json.loads(s)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            pass
    start, end = s.find("{"), s.rfind("}")
    if 0 <= start < end:
        try:
            parsed = json.loads(s[start : end + 1])
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            pass
    return {}


def _anthropic_messages_text(
    *, system: str, user_msg: str, model: str, max_tokens: int
) -> str:
    api_key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY manquante")
    base = (os.environ.get("ANTHROPIC_BASE_URL") or "https://api.anthropic.com/v1").strip().rstrip("/")
    # Note: newer models (Opus 4.8+) reject the `temperature` param ("deprecated for
    # this model"); we omit it and rely on the model default.
    resp = requests.post(
        f"{base}/messages",
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "max_tokens": max_tokens,
            "system": system,
            "messages": [{"role": "user", "content": user_msg}],
        },
        timeout=90,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Anthropic HTTP {resp.status_code}: {resp.text[:300]}")
    data = resp.json()
    parts = data.get("content") if isinstance(data, dict) else None
    if not isinstance(parts, list):
        return ""
    out: list[str] = []
    for p in parts:
        if isinstance(p, dict) and p.get("type") == "text":
            out.append(str(p.get("text") or ""))
    return "".join(out).strip()


def _openai_chat_text(
    *, system: str, user_msg: str, model: str, max_tokens: int, temperature: float, json_mode: bool
) -> str:
    api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY manquante")
    base = (os.environ.get("OPENAI_BASE_URL") or "https://api.openai.com/v1").strip().rstrip("/")
    payload: dict[str, Any] = {
        "model": model,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user_msg},
        ],
    }
    if json_mode:
        payload["response_format"] = {"type": "json_object"}
    resp = requests.post(
        f"{base}/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=payload,
        timeout=90,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"OpenAI HTTP {resp.status_code}: {resp.text[:300]}")
    data = resp.json()
    return str(data["choices"][0]["message"]["content"] or "")


def _correction_ai_json(
    *, system: str, user_msg: str, max_tokens: int = 4000, temperature: float = 0.05,
    error_sink: list[str] | None = None, model_override: str = "",
) -> dict[str, Any]:
    """Generate a JSON object using the best available correction AI.

    Tries the preferred provider first (Claude by default), then falls back to the
    other configured provider if the primary call fails. Returns {} when none work.
    `model_override` forces the Anthropic model (per-plan: Sonnet vs Opus).
    Failures are logged and (optionally) appended to error_sink for surfacing.
    """
    primary = _correction_ai_provider()
    if primary == "none":
        if error_sink is not None:
            error_sink.append("Aucune clé IA configurée (ANTHROPIC_API_KEY ou OPENAI_API_KEY).")
        return {}
    order = [primary]
    if primary == "anthropic" and (os.environ.get("OPENAI_API_KEY") or "").strip():
        order.append("openai")
    elif primary == "openai" and (os.environ.get("ANTHROPIC_API_KEY") or "").strip():
        order.append("anthropic")
    json_hint = "\n\nRéponds UNIQUEMENT avec l'objet JSON valide, sans texte autour ni bloc markdown."
    for prov in order:
        model = (model_override if (prov == "anthropic" and model_override) else _correction_ai_model(prov))
        if model_override and prov != "anthropic":
            # The per-plan model (Sonnet for Solo/Pro, Opus for Business) only applies to the
            # Anthropic path. With no ANTHROPIC_API_KEY the whole tier differentiation is
            # silently dropped and every plan gets the OpenAI fallback — a 199 EUR customer runs
            # the same engine as a 49 EUR one. Say so, once per call: this project has already
            # lost a whole SDK major to a failure that logged nothing.
            logger.warning(
                "[correction-ai] per-plan model %r ignored: provider is %s (set ANTHROPIC_API_KEY "
                "to honour the plan tier, or drop `correction.model` from the catalogue)",
                model_override, prov,
            )
        if not model:
            continue
        try:
            if prov == "anthropic":
                text = _anthropic_messages_text(
                    system=system + json_hint,
                    user_msg=user_msg,
                    model=model,
                    max_tokens=max_tokens,
                )
            else:
                text = _openai_chat_text(
                    system=system,
                    user_msg=user_msg,
                    model=model,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    json_mode=True,
                )
            parsed = _parse_ai_json(text)
            if parsed:
                return parsed
            msg = f"{prov} ({model}): réponse vide ou non-JSON"
            logger.warning("[correction-ai] %s", msg)
            if error_sink is not None:
                error_sink.append(msg)
        except Exception as e:
            msg = f"{prov} ({model}): {e}"
            logger.warning("[correction-ai] %s", msg)
            if error_sink is not None:
                error_sink.append(msg)
            continue
    return {}


def _ai_configured() -> bool:
    """True when at least one generation provider (Claude or OpenAI) is available."""
    return _correction_ai_provider() != "none"


def _ai_generate_text(
    *, system: str, user_msg: str, max_tokens: int = 800, temperature: float = 0.7,
    error_sink: list[str] | None = None,
) -> str:
    """Free-text generation via the best available AI (Claude preferred, OpenAI fallback).

    Mirrors _correction_ai_json but returns raw text (no JSON). Returns "" on failure.
    """
    primary = _correction_ai_provider()
    if primary == "none":
        if error_sink is not None:
            error_sink.append("Aucune clé IA configurée (ANTHROPIC_API_KEY ou OPENAI_API_KEY).")
        return ""
    order = [primary]
    if primary == "anthropic" and (os.environ.get("OPENAI_API_KEY") or "").strip():
        order.append("openai")
    elif primary == "openai" and (os.environ.get("ANTHROPIC_API_KEY") or "").strip():
        order.append("anthropic")
    for prov in order:
        model = _correction_ai_model(prov)
        if not model:
            continue
        try:
            if prov == "anthropic":
                text = _anthropic_messages_text(
                    system=system, user_msg=user_msg, model=model, max_tokens=max_tokens,
                )
            else:
                text = _openai_chat_text(
                    system=system, user_msg=user_msg, model=model,
                    max_tokens=max_tokens, temperature=temperature, json_mode=False,
                )
            if text.strip():
                return text.strip()
            msg = f"{prov} ({model}): réponse vide"
            logger.warning("[ai-text] %s", msg)
            if error_sink is not None:
                error_sink.append(msg)
        except Exception as e:
            msg = f"{prov} ({model}): {e}"
            logger.warning("[ai-text] %s", msg)
            if error_sink is not None:
                error_sink.append(msg)
            continue
    return ""


def _openai_url_fix(
    *,
    issue_key: str,
    issue_label: str,
    url: str,
    site_name: str,
    error_sink: list[str] | None = None,
    model_override: str = "",
) -> dict[str, Any]:
    system = (
        "Tu es un expert SEO technique qui génère des corrections CODE-READY, immédiatement applicables.\n"
        "Pour chaque anomalie et URL, produis une correction technique précise avec le code exact.\n\n"
        "EXEMPLES DE CODE À GÉNÉRER selon l'anomalie :\n"
        "- redirect_3xx HTTP→HTTPS : RewriteRule .htaccess ou return 301 nginx\n"
        "- redirect_3xx www→non-www : règle de redirection canonique\n"
        "- missing_meta_description : balise <meta name=\"description\" content=\"...\"> à ajouter\n"
        "- missing_title : balise <title>...</title> à ajouter/modifier\n"
        "- broken_link / http_404 : identifier l'URL cible correcte, proposer une redirection 301\n"
        "- duplicate_content : balise <link rel=\"canonical\" href=\"...\"> à ajouter\n"
        "- missing_h1 / multiple_h1 : balise <h1> à ajouter ou corriger dans le HTML\n"
        "- image_missing_alt : attribut alt=\"...\" à ajouter sur la balise <img>\n"
        "- slow_page : en-têtes Cache-Control ou config de compression à appliquer\n\n"
        "Réponds STRICTEMENT en JSON avec ces 3 champs :\n"
        "{\n"
        "  \"fix\": \"Diagnostic précis : quel est le problème exact sur cette URL (1-2 phrases)\",\n"
        "  \"code\": \"Le code exact à copier-coller (.htaccess, nginx.conf, balise HTML, etc.)\",\n"
        "  \"action\": \"Où et comment appliquer ce code (fichier cible, étape précise)\"\n"
        "}\n"
        "Le champ 'code' doit contenir du vrai code, pas une description. Si plusieurs variantes "
        "(Apache/Nginx/Cloudflare), donne la plus universelle. Ne mets JAMAIS de texte vague."
    )
    user_msg = json.dumps({
        "site": site_name,
        "anomalie": f"{issue_key} — {issue_label}",
        "url_affectee": url,
    }, ensure_ascii=False)
    parsed = _correction_ai_json(
        system=system, user_msg=user_msg, max_tokens=900, temperature=0.1, error_sink=error_sink,
        model_override=model_override,
    )
    if isinstance(parsed, dict) and parsed.get("fix"):
        return parsed
    return {}


# Maps issue_key → ordered list of candidate file names/patterns to look for in the repo
_SEO_FILE_CANDIDATES: dict[str, list[str]] = {
    "redirect_3xx": [
        "next.config.js", "next.config.ts", "next.config.mjs", "vercel.json", "netlify.toml", "_headers",
        ".htaccess", "nginx.conf", "wrangler.toml",
    ],
    "missing_meta_description": [
        "app/layout.tsx", "src/app/layout.tsx", "pages/_document.tsx", "pages/_app.tsx", "layout.html",
        "base.html", "_layout.html", "app.html", "_document.tsx", "_document.jsx", "index.html", "layout.tsx",
    ],
    "missing_title": [
        "app/layout.tsx", "src/app/layout.tsx", "pages/_document.tsx", "pages/_app.tsx", "layout.html",
        "base.html", "_layout.html", "app.html", "_document.tsx", "_document.jsx", "index.html", "layout.tsx",
    ],
    "missing_h1": ["app/page.tsx", "src/app/page.tsx", "pages/index.tsx", "layout.html", "base.html", "index.html"],
    "multiple_h1": ["app/page.tsx", "src/app/page.tsx", "pages/index.tsx", "layout.html", "base.html", "index.html"],
    "image_missing_alt": ["app/page.tsx", "src/app/page.tsx", "pages/index.tsx", "index.html", "layout.html"],
    "slow_page": ["next.config.js", "next.config.ts", "vercel.json", "netlify.toml", "_headers", ".htaccess"],
}
_SEO_FILE_CANDIDATES_DEFAULT = [
    "app/layout.tsx", "src/app/layout.tsx", "app/page.tsx", "src/app/page.tsx",
    "pages/_document.tsx", "pages/_app.tsx", "pages/index.tsx",
    "vercel.json", "netlify.toml", ".htaccess", "_headers", "next.config.js", "next.config.ts",
    "layout.html", "base.html", "index.html",
]


_SITEMAP_FILE_CANDIDATES = [
    "app/sitemap.ts", "app/sitemap.js", "src/app/sitemap.ts", "src/app/sitemap.js",
    "app/sitemap.xml/route.ts", "next-sitemap.config.js", "next-sitemap.config.cjs",
    "public/sitemap.xml", "sitemap.xml", "nuxt.config.ts",
]


_HREFLANG_FILE_CANDIDATES = [
    "app/layout.tsx", "src/app/layout.tsx", "app/[lang]/layout.tsx", "app/[locale]/layout.tsx",
    "pages/_document.tsx", "lib/seo.ts", "lib/metadata.ts", "lib/hreflang.ts", "i18n.ts",
    "layout.html", "base.html",
]
_HEAD_FILE_CANDIDATES = [
    "app/layout.tsx", "src/app/layout.tsx", "pages/_document.tsx", "pages/_app.tsx",
    "layout.html", "base.html", "index.html", "head.tsx", "seo.tsx",
]
_CANONICAL_FILE_CANDIDATES = [
    "app/layout.tsx", "src/app/layout.tsx", "pages/_document.tsx", "layout.html", "base.html",
    "index.html", "head.tsx", "seo.tsx",
]
_PAGE_CONTENT_FILE_CANDIDATES = [
    "app/page.tsx", "src/app/page.tsx", "pages/index.tsx", "index.html", "layout.html", "base.html",
]
_REDIRECT_CONFIG_FILE_CANDIDATES = _SEO_FILE_CANDIDATES["redirect_3xx"]
_ASSET_FILE_CANDIDATES = _SEO_FILE_CANDIDATES["image_missing_alt"]


def _issue_file_families() -> "list[tuple[str, set[str], list[str]]]":
    """Explicit key → candidate-files declaration, one entry per family a corrector CLAIMS.

    Built from the handler tables themselves, so a family's files come from the same place that
    claims its keys — one source of truth instead of two that can drift. Evaluated at call time
    because those tables are defined further down this module.

    This exists because the legacy resolver below matches ORDERED SUBSTRINGS, and that silently
    mis-routed three families at once: `sitemap_3xx_redirect` hit the "redirect" branch and got
    netlify.toml, `sitemap_non_canonical_page` hit "canonical" and got a layout, the hreflang↔
    sitemap conflict hit "hreflang" and got lib/seo.ts. Two of them had been broken since the day
    they shipped. Explicit sets cannot mis-route, and a test asserts they stay disjoint."""
    length_keys: set[str] = set()
    for family in _LENGTH_FAMILIES.values():
        length_keys |= set(family)

    # Some keys legitimately appear in two handler tables: a canonical-tag issue is listed in
    # _HEAD_HINTS AND is a canonical fix; hreflang_to_non_canonical is a hreflang tag whose name
    # says "canonical". Ownership is therefore SUBTRACTED here rather than left to list order —
    # the whole point of this table is that no key resolves differently depending on position.
    sitemap = set(_SITEMAP_FAMILY_KEYS)
    redirect_config = set(_REDIRECT_CONFIG_KEYS)
    links = set(_REDIRECT_LINK_KEYS) | set(_MIXED_CONTENT_KEYS) | set(_DOUBLE_SLASH_KEYS)
    assets = set(_ASSET_REWRITE_KEYS) | {"missing_alt_text"}
    hreflang = set(_HREFLANG_HINTS)                      # a hreflang tag, whatever its name says
    canonical = ({k for k in _URL_PAIR_KEYS if "canonical" in k}
                 | {"missing_canonical", "duplicate_pages_without_canonical"}) - hreflang
    head = set(_HEAD_HINTS) - canonical - hreflang       # the rest of the <head>: OG, twitter, viewport
    content = (length_keys | {
        "missing_title", "missing_meta_description", "missing_h1",
        "duplicate_titles", "duplicate_meta_descriptions",
        "multiple_title_tags", "multiple_meta_description_tags", "multiple_h1",
    }) - head - canonical
    return [
        ("sitemap", sitemap, _SITEMAP_FILE_CANDIDATES),
        ("redirect-config", redirect_config, _REDIRECT_CONFIG_FILE_CANDIDATES),
        ("links", links, _PAGE_CONTENT_FILE_CANDIDATES),
        ("assets", assets, _ASSET_FILE_CANDIDATES),
        ("hreflang", hreflang, _HREFLANG_FILE_CANDIDATES),
        ("canonical", canonical, _CANONICAL_FILE_CANDIDATES),
        ("head", head, _HEAD_FILE_CANDIDATES),
        ("content", content, _PAGE_CONTENT_FILE_CANDIDATES),
    ]


def _claimed_family_candidates(key: str) -> "list[str] | None":
    """Candidate files for a key a corrector claims, or None when no family declares it."""
    for _name, keys, files in _issue_file_families():
        if key in _with_indexability_variants(keys):
            return files
    return None


def _seo_file_candidates_for_issue(issue_key: str) -> list[str]:
    key = (issue_key or "").strip().lower()
    # Claimed families resolve by EXPLICIT declaration. Everything below is the legacy substring
    # chain, kept only for keys nobody claims: those are advisory, so their candidates are never
    # consulted by the fix path and a mis-route there cannot reach a repository.
    declared = _claimed_family_candidates(key)
    if declared is not None:
        return declared
    if key in _SEO_FILE_CANDIDATES:
        return _SEO_FILE_CANDIDATES[key]
    if key in {"duplicate_titles", "multiple_title_tags"} or key.startswith("title_too_"):
        return _SEO_FILE_CANDIDATES["missing_title"]
    if (
        key in {"duplicate_meta_descriptions", "multiple_meta_description_tags"}
        or key.startswith("meta_description_")
    ):
        return _SEO_FILE_CANDIDATES["missing_meta_description"]
    if key.startswith("missing_h1") or key in {"multiple_h1", "h1_tag_changed"}:
        return _SEO_FILE_CANDIDATES["missing_h1"]
    if "canonical" in key:
        return [
            "app/layout.tsx", "src/app/layout.tsx", "pages/_document.tsx", "layout.html", "base.html",
            "index.html", "head.tsx", "seo.tsx",
        ]
    if "redirect" in key or key in {"http_404", "http_4xx", "links_to_404_page", "links_to_4xx_page"}:
        return _SEO_FILE_CANDIDATES["redirect_3xx"]
    if "sitemap" in key:
        return _SITEMAP_FILE_CANDIDATES
    if "robots" in key or "noindex" in key or "nofollow" in key:
        return [
            "app/robots.ts", "app/robots.js", "src/app/robots.ts", "public/robots.txt",
            "robots.txt", "app/layout.tsx", "src/app/layout.tsx", "layout.html", "base.html", "_headers",
        ]
    if "open_graph" in key or "twitter_card" in key:
        return ["app/layout.tsx", "src/app/layout.tsx", "pages/_document.tsx", "layout.html", "base.html", "seo.tsx"]
    if "hreflang" in key or "html_lang" in key or key.endswith("_lang_missing"):
        return [
            "app/layout.tsx", "src/app/layout.tsx", "app/[lang]/layout.tsx", "app/[locale]/layout.tsx",
            "pages/_document.tsx", "lib/seo.ts", "lib/metadata.ts", "lib/hreflang.ts", "i18n.ts",
            "layout.html", "base.html",
        ]
    if "structured" in key or "schema" in key:
        return ["app/layout.tsx", "src/app/layout.tsx", "layout.html", "base.html", "schema.ts", "seo.tsx"]
    if "viewport" in key:
        return [
            "app/layout.tsx", "src/app/layout.tsx", "pages/_document.tsx", "pages/_app.tsx",
            "layout.html", "base.html", "index.html", "head.tsx",
        ]
    if "javascript" in key or "css" in key:
        return ["next.config.js", "next.config.ts", "vercel.json", "netlify.toml", "_headers", "package.json"]
    if "image" in key or "alt" in key:
        return _SEO_FILE_CANDIDATES["image_missing_alt"]
    if key in {"low_word_count", "page_and_serp_titles_do_not_match"}:
        return ["app/page.tsx", "src/app/page.tsx", "pages/index.tsx", "index.html", "content.ts", "seo.tsx"]
    return _SEO_FILE_CANDIDATES_DEFAULT


# Issues with no dedicated handler that a plain per-page content patch can still fix: the value
# to write is derivable from the page itself, and the file to touch is the page's own source.
# Anything NOT listed here and not claimed by a handler stays advisory — see below.
_GENERIC_CONTENT_FIX_KEYS = {
    "missing_title", "missing_meta_description", "missing_h1",
    "duplicate_titles", "duplicate_meta_descriptions",
    "multiple_meta_description_tags", "duplicate_pages_without_canonical",
    "missing_canonical",
}


def _with_indexability_variants(keys: "set[str] | frozenset[str] | tuple[str, ...]") -> set[str]:
    """Add the Indexable / Not-indexable twins of every key.

    Ahrefs splits many issues along indexability and the crawler emits the SUFFIXED keys —
    `missing_h1_indexable`, never a bare `missing_h1`. A handler that declares only the base
    therefore never fires: a freshly injected missing-h1 defect showed up in Anomalies and never
    reached the corrections page. The length and links-to-redirect families had spelled their
    variants out by hand; deriving them removes the chance to forget."""
    out = set(keys)
    for key in keys:
        if key.endswith(("_indexable", "_not_indexable")):
            continue
        out.add(f"{key}_indexable")
        out.add(f"{key}_not_indexable")
    return out


def _handled_issue_keys() -> set[str]:
    """Every issue key some corrector actually claims. Built at call time because the handler
    tables are defined further down this module."""
    handled: set[str] = set(_GENERIC_CONTENT_FIX_KEYS)
    for table in (_HEAD_HINTS, _HREFLANG_HINTS):
        handled |= set(table)
    for group in (
        _SITEMAP_ADD_KEYS, _SITEMAP_REWRITE_KEYS, _SITEMAP_ALTERNATE_KEYS, _URL_PAIR_KEYS, _ASSET_REWRITE_KEYS,
        _REDIRECT_LINK_KEYS, _MIXED_CONTENT_KEYS, _DOUBLE_SLASH_KEYS, _PAGE_VALUE_KEYS,
        _REDIRECT_CONFIG_KEYS,
    ):
        handled |= set(group)
    for family in _LENGTH_FAMILIES.values():
        handled |= set(family)
    handled.add("missing_alt_text")
    return _with_indexability_variants(handled)


def _github_issue_auto_fixable(issue_key: str) -> bool:
    """An issue is offered for auto-fix ONLY when a corrector claims it.

    This used to be the opposite: anything whose file-candidate lookup returned a non-default
    list was considered fixable, which silently made 85 of the 191 catalogued issues eligible
    for a free-form AI patch — including `redirect_3xx` (candidates: netlify.toml, the file
    holding HSTS and CSP) and the sitemap hygiene family. Both were real hazards found in
    testing, both instances of the same permissive default. Opt-in closes the class: a new
    issue key is advisory until someone writes a handler and adds it to `_handled_issue_keys`."""
    key = (issue_key or "").strip().lower()
    if not key:
        return False
    # Advisory-only issues: content quality, Core Web Vitals / perf, external targets, crawl
    # timeouts and proprietary rank/traffic metrics can't be fixed by a mechanical code patch.
    # Kept ahead of the allow-list so a key can never be claimed by accident.
    if key in _ADVISORY_ISSUE_KEYS or any(tok in key for tok in _ADVISORY_ISSUE_TOKENS):
        return False
    return key in _handled_issue_keys()


# Issues that are real but NOT mechanically code-fixable — the agent advises instead of patching.
_ADVISORY_ISSUE_KEYS = {
    "low_word_count", "slow_page", "page_size_exceeds_2mb", "content_is_not_sized_correctly",
    "font_size_too_small", "tap_targets_too_small_or_close", "not_compressed", "timed_out",
    "page_from_sitemap_timed_out", "orphan_page_indexable", "orphan_page_not_indexable",
}
_ADVISORY_ISSUE_TOKENS = (
    "word_count", "poor_cls", "poor_fid", "poor_inp", "poor_lcp", "cwv", "core_web_vital",
    "high_ai_content", "organic_traffic", "referring_domain", "serp_title", "and_serp_titles",
    "dropped_from_top", "receives_organic",
)


def _project_github_cfg(proj) -> dict[str, str]:
    s = proj.settings if isinstance(proj.settings, dict) else {}
    mode = str(s.get("github_mode") or "review").strip()
    return {
        "repo": str(s.get("github_repo") or "").strip(),
        "branch": str(s.get("github_branch") or "main").strip(),
        "mode": mode if mode in {"review", "auto"} else "review",
    }


_EDITABLE_EXTS = {
    "html", "htm", "tsx", "jsx", "ts", "js", "mjs", "cjs", "vue", "svelte", "astro",
    "php", "md", "mdx", "json", "toml", "yaml", "yml", "conf", "liquid", "ejs", "hbs", "twig", "erb", "xml",
}
_is_repo_noise = repo_index.is_noise_path


def _ai_pick_repo_files(issue_key: str, issue_label: str, all_paths: list[str], *, limit: int = 2) -> list[str]:
    """Fallback when no hardcoded candidate matches: let the AI pick the most relevant
    file(s) to edit from the repo's actual file tree. Returns validated repo-relative paths."""
    if not all_paths:
        return []
    cand: list[str] = []
    for p in all_paths:
        low = p.lower()
        if _is_repo_noise(p):
            continue
        base = low.rsplit("/", 1)[-1]
        ext = base.rsplit(".", 1)[-1] if "." in base else ""
        if ext in _EDITABLE_EXTS or base in {".htaccess", "_headers", "robots.txt", "nginx.conf"}:
            cand.append(p)
    if not cand:
        return []
    system = (
        "Tu es un expert SEO/dev. On te donne la liste des fichiers d'un dépôt et une anomalie SEO. "
        "Choisis le(s) fichier(s) LE(S) PLUS PERTINENT(S) à éditer pour corriger cette anomalie "
        "(template, layout, page d'accueil, config serveur selon le cas). "
        "Réponds STRICTEMENT en JSON : {\"files\": [\"chemin/relatif\"]} — 1 à 3 chemins EXACTEMENT tels "
        "qu'ils apparaissent dans la liste, par ordre de pertinence. Si rien ne convient: {\"files\": []}."
    )
    user_msg = json.dumps(
        {"anomalie": f"{issue_key} — {issue_label}", "fichiers": cand[:400]}, ensure_ascii=False
    )
    parsed = _correction_ai_json(system=system, user_msg=user_msg, max_tokens=400, temperature=0.0)
    files = parsed.get("files") if isinstance(parsed, dict) else None
    out: list[str] = []
    if isinstance(files, list):
        allow = set(all_paths)
        for f in files:
            fs = str(f).strip()
            if fs in allow and fs not in out:
                out.append(fs)
            if len(out) >= limit:
                break
    return out


def _ai_map_urls_to_files(
    *, issue_key: str, issue_label: str, urls: list[str], all_paths: list[str], limit: int = 8,
    evidence: list[str] | None = None,
) -> list[str]:
    """Map a set of impacted URLs to the source file(s) that must be edited to fix them all.

    Used for high-occurrence issues: prefer a single shared template/component/config when it
    covers every URL; otherwise return the distinct page files behind the impacted URLs."""
    if not all_paths or not urls:
        return []
    cand: list[str] = []
    for p in all_paths:
        low = p.lower()
        if _is_repo_noise(p):
            continue
        base = low.rsplit("/", 1)[-1]
        ext = base.rsplit(".", 1)[-1] if "." in base else ""
        if ext in _EDITABLE_EXTS or base in {".htaccess", "_headers", "robots.txt", "nginx.conf"}:
            cand.append(p)
    if not cand:
        return []
    system = (
        "Tu es un expert SEO/dev. On te donne la liste des fichiers d'un dépôt, une anomalie SEO, "
        "et la liste des URLs du site touchées par cette anomalie. "
        "Détermine LE PLUS PETIT ensemble de fichiers source à éditer pour corriger l'anomalie sur "
        "TOUTES ces URLs.\n"
        "- Si un template/composant/layout/config partagé couvre toutes les URLs, renvoie CE seul fichier.\n"
        "- Sinon, renvoie le fichier source de chaque page impactée (mappe chaque URL à son fichier : "
        "route Next.js, page statique, template, etc.).\n"
        "Si des 'indices' précis sont fournis (ex. src d'images), trouve les fichiers qui RÉFÉRENCENT "
        "ces indices.\n"
        "Réponds STRICTEMENT en JSON : {\"files\": [\"chemin/relatif\"]} — chemins EXACTEMENT tels qu'ils "
        "apparaissent dans la liste, par ordre de priorité. Si rien ne convient: {\"files\": []}."
    )
    payload_map: dict[str, Any] = {
        "anomalie": f"{issue_key} — {issue_label}",
        "urls_impactees": urls[:40],
        "fichiers_du_depot": cand[:500],
    }
    if evidence:
        payload_map["indices_precis"] = evidence[:30]
    user_msg = json.dumps(payload_map, ensure_ascii=False)
    parsed = _correction_ai_json(system=system, user_msg=user_msg, max_tokens=700, temperature=0.0)
    files = parsed.get("files") if isinstance(parsed, dict) else None
    out: list[str] = []
    if isinstance(files, list):
        allow = set(all_paths)
        for f in files:
            fs = str(f).strip()
            if fs in allow and fs not in out:
                out.append(fs)
            if len(out) >= limit:
                break
    return out


def _github_find_seo_files(
    owner: str, repo: str, branch: str, token: str, issue_key: str
) -> list[dict[str, Any]]:
    """Return [{path, content_str, sha}] for the most relevant SEO file in the repo."""
    if _github_repo_parts(f"{owner}/{repo}") is None:
        raise RuntimeError("Dépôt GitHub invalide.")
    if not _github_branch_allowed(branch):
        raise RuntimeError("Branche GitHub invalide.")
    try:
        tree_data = _github_api_get(
            _github_api_path("repos", owner, repo, "git", "trees", branch),
            token=token,
            params={"recursive": "1"},
            timeout_s=20,
        )
    except Exception as e:
        raise RuntimeError(f"Impossible de lire l'arbre du repo : {e}") from e

    all_paths: list[str] = [
        item["path"] for item in (tree_data.get("tree") or [])
        if isinstance(item, dict) and item.get("type") == "blob" and _github_file_path_allowed(str(item.get("path") or ""))
    ]

    candidates = _seo_file_candidates_for_issue(issue_key)
    matches: list[str] = []
    for candidate in candidates:
        for p in all_paths:
            filename = p.split("/")[-1]
            if (p == candidate or p.endswith(f"/{candidate}") or filename == candidate) and p not in matches:
                matches.append(p)
                break
    if not matches:
        # No hardcoded candidate exists in this repo — let the AI pick from the real tree.
        meta = dash.issue_meta(issue_key)
        issue_label = meta.label if meta else issue_key
        matches = _ai_pick_repo_files(issue_key, issue_label, all_paths)
    if not matches:
        raise RuntimeError(
            "Aucun fichier corrigeable trouvé pour cette anomalie dans le dépôt. "
            "Vérifie que le dépôt connecté contient bien le code source du site."
        )

    results: list[dict[str, Any]] = []
    for path in matches[:2]:
        try:
            file_data = _github_api_get(_github_content_api_path(owner, repo, path), token=token, params={"ref": branch})
            import base64 as _b64
            raw = _b64.b64decode(file_data.get("content", "").replace("\n", "")).decode("utf-8", errors="replace")
            if len(raw) > 80_000:
                raw = raw[:80_000]
            results.append({"path": path, "content": raw, "sha": file_data.get("sha", "")})
        except Exception:
            continue
    if not results:
        raise RuntimeError("Impossible de lire le contenu des fichiers trouvés.")
    return results


# Shared "what to change" rules — used by BOTH the targeted-edit and full-file patch prompts
# so they stay consistent (every hardening rule applies regardless of output format).
_PATCH_RULES = (
    "- Modifie UNIQUEMENT les éléments réellement non conformes DANS CE FICHIER. "
    "Juge la conformité élément par élément d'après le contenu de CE fichier, pas d'après le "
    "contexte global.\n"
    "- Cas 'alt manquant' : un <img> est NON conforme SEULEMENT si, dans ce fichier, son attribut "
    "alt est ABSENT ou VIDE (alt=\"\") → tu DOIS alors renseigner un alt court et descriptif "
    "(ex. <img src=\"/images/btc.svg\"> → alt=\"Bitcoin\"). IMPORTANT : remplis l'alt vide MÊME si "
    "l'image paraît décorative ou porte aria-hidden=\"true\" — l'outil d'audit compte tout alt vide "
    "comme manquant. Ne laisse JAMAIS alt=\"\" (garde aria-hidden s'il est présent, change juste l'alt). "
    "En revanche, un <img> qui a DÉJÀ un alt NON VIDE est CONFORME → n'y touche pas : ne le reformule "
    "pas, ne le traduis pas, ne l'enrichis pas. Les 'src d'images' donnés en contexte manquent d'alt "
    "AILLEURS sur le site — cela NE veut PAS dire qu'ils manquent d'alt dans ce fichier-ci.\n"
    "- Si AUCUN <img> de ce fichier n'a un alt absent/vide, mets no_change=true sans rien modifier.\n"
    "- Corrige TOUTES les occurrences réellement non conformes présentes dans CE fichier (pas seulement la première)\n"
    "- Si AUCUN élément de ce fichier ne présente l'anomalie, mets no_change=true. Ne fabrique pas de correction artificielle.\n"
    "- Ne modifie RIEN d'autre que ce qui est strictement nécessaire\n"
    "- Cas longueur (title / meta description) : vise la fenêtre OPTIMALE et NE LA DÉPASSE PAS — "
    "title ≈ 50-60 caractères, meta description ≈ 120-160 caractères. Pour un 'trop court', allonge "
    "juste assez pour entrer dans la fenêtre (ne survends pas) ; pour un 'trop long', raccourcis dans "
    "la fenêtre. ÉVITE ABSOLUMENT les changements GLOBAUX qui affectent d'autres pages (ex. un template "
    "de titre `%s | Marque` rallonge TOUTES les pages et en casse certaines) — corrige page par page, "
    "uniquement les titres réellement hors-fenêtre.\n"
    "- Champ ciblé UNIQUEMENT : pour une anomalie de TITRE, modifie EXCLUSIVEMENT le titre "
    "(frontmatter `title:` / balise <title> / champ title) ; pour une anomalie de META DESCRIPTION, "
    "modifie EXCLUSIVEMENT la description. Ne touche JAMAIS à l'autre champ, ni au reste du frontmatter, "
    "ni au corps du contenu, ni aux caractères invisibles (BOM) — laisse le fichier identique ailleurs.\n"
    "- DANGER — expression dynamique : si le titre (ou la meta) est une EXPRESSION dérivée des données "
    "de la page (ex. `post.title`, `{frontmatter.title}`, une variable, `data.xxx`, `generateMetadata`), "
    "NE la remplace JAMAIS par une chaîne statique et n'y concatène JAMAIS de suffixe — tu donnerais le "
    "MÊME titre à TOUTES les pages de cette route (catastrophe SEO). Dans un fichier de route partagé "
    "(`[slug]`, `[...slug]`, layout, _app, _document), laisse l'expression dynamique INTACTE → no_change, "
    "et corrige plutôt la SOURCE de chaque page (son frontmatter / contenu).\n"
    "- Cas 'lien vers redirection' : on te fournit des paires lien→destination finale. Remplace "
    "EXACTEMENT chaque URL de lien indiquée par sa destination finale fournie (ex. href=\"/x/\" → "
    "href=\"/x\"). NE modifie QUE ces liens-là, à l'identique ailleurs ; ne crée JAMAIS de nouvelle "
    "redirection (n'inverse pas le sens) ; n'invente aucune URL hors des paires fournies.\n"
    "- Adapte la syntaxe au format du fichier (JSON, TOML, .htaccess, JS, HTML, etc.)\n"
    "- Ne casse JAMAIS la syntaxe : un JSON doit rester un JSON valide, un TOML un TOML valide, etc."
)


def _patch_user_msg(file_path: str, file_content: str, issue_key: str, issue_label: str, url: str, site_name: str, occurrences_hint: str) -> str:
    payload_obj: dict[str, Any] = {
        "site": site_name,
        "anomalie": f"{issue_key} — {issue_label}",
        "url_affectee": url,
        "fichier": file_path,
        "contenu_actuel": file_content,
    }
    if occurrences_hint:
        payload_obj["contexte"] = occurrences_hint
    return json.dumps(payload_obj, ensure_ascii=False)


def _apply_edits(content: str, edits: Any) -> tuple[str, int]:
    """Apply [{old,new}] find/replace edits. Replaces ALL occurrences of each exact `old`
    (so identical occurrences are all fixed at once). Returns (new_content, edits_applied)."""
    out = content
    applied = 0
    if not isinstance(edits, list):
        return out, 0
    for e in edits:
        if not isinstance(e, dict):
            continue
        old, new = e.get("old"), e.get("new")
        if not isinstance(old, str) or not isinstance(new, str) or not old or old == new:
            continue
        if old in out:
            out = out.replace(old, new)
            applied += 1
    return out, applied


def _patch_via_edits(*, file_path, file_content, issue_key, issue_label, url, site_name, occurrences_hint, model_override) -> dict[str, Any]:
    """Cheap path: ask for targeted find/replace edits (small output) and apply them locally."""
    system = (
        "Tu es un expert SEO technique. On te donne le contenu COMPLET d'un fichier, une anomalie SEO "
        "et l'URL affectée. Tu renvoies des ÉDITIONS CIBLÉES (find/replace), PAS le fichier entier.\n\n"
        "Réponds STRICTEMENT en JSON :\n"
        "{\n"
        '  "pr_title": "fix: [description courte] (max 72 chars)",\n'
        '  "description": "Explication en 2-3 phrases",\n'
        '  "no_change": false,\n'
        '  "edits": [ {"old": "<texte EXACT à remplacer, copié tel quel du fichier avec assez de contexte pour être unique>", "new": "<remplacement>"} ]\n'
        "}\n"
        "- 'old' DOIT être une sous-chaîne EXACTE du fichier (mêmes espaces, guillemets, casse), assez longue pour être trouvée sans ambiguïté.\n"
        "- Une édition par modification distincte. Pour plusieurs occurrences IDENTIQUES, une seule édition suffit (toutes les occurrences de 'old' sont remplacées).\n"
        "- Ne renvoie pas le fichier entier ; uniquement les morceaux qui changent.\n"
        "- no_change=true et edits=[] s'il n'y a rien à corriger dans CE fichier.\n\n"
        "RÈGLES ABSOLUES (sur QUOI changer) :\n" + _PATCH_RULES
    )
    user_msg = _patch_user_msg(file_path, file_content, issue_key, issue_label, url, site_name, occurrences_hint)
    parsed = _correction_ai_json(system=system, user_msg=user_msg, max_tokens=2000, temperature=0.05, model_override=model_override)
    if not isinstance(parsed, dict):
        return {}
    if parsed.get("no_change"):
        return {"no_change": True, "pr_title": parsed.get("pr_title", ""), "description": parsed.get("description", "")}
    new_content, applied = _apply_edits(file_content, parsed.get("edits"))
    if applied == 0 or new_content == file_content:
        return {}  # nothing matched/applied → caller falls back to full-file
    return {"patched_content": new_content, "pr_title": parsed.get("pr_title", ""), "description": parsed.get("description", "")}


def _patch_via_full_file(*, file_path, file_content, issue_key, issue_label, url, site_name, occurrences_hint, model_override) -> dict[str, Any]:
    """Reliable fallback: regenerate the COMPLETE file (more output tokens)."""
    system = (
        "Tu es un expert SEO technique qui génère des patches de code précis et applicables.\n"
        "On te donne le contenu COMPLET d'un fichier, une anomalie SEO précise et l'URL affectée.\n"
        "Tu dois retourner le fichier COMPLET modifié (pas un diff, le fichier entier).\n\n"
        "Réponds STRICTEMENT en JSON :\n"
        "{\n"
        '  "pr_title": "fix: [description courte] (max 72 chars)",\n'
        '  "description": "Explication en 2-3 phrases",\n'
        '  "patched_content": "contenu complet du fichier après correction",\n'
        '  "no_change": false\n'
        "}\n\n"
        "RÈGLES ABSOLUES :\n"
        "- patched_content = fichier COMPLET, pas juste le bloc modifié, sans rien tronquer\n"
        + _PATCH_RULES
    )
    user_msg = _patch_user_msg(file_path, file_content, issue_key, issue_label, url, site_name, occurrences_hint)
    parsed = _correction_ai_json(system=system, user_msg=user_msg, max_tokens=8000, temperature=0.05, model_override=model_override)
    if not isinstance(parsed, dict):
        return {}
    if parsed.get("no_change"):
        return {"no_change": True, "pr_title": parsed.get("pr_title", ""), "description": parsed.get("description", "")}
    return parsed if parsed.get("patched_content") else {}


def _openai_generate_file_patch(
    *,
    file_path: str,
    file_content: str,
    issue_key: str,
    issue_label: str,
    url: str,
    site_name: str,
    occurrences_hint: str = "",
    model_override: str = "",
) -> dict[str, Any]:
    # #4 safety: a file that hit the 80KB read cap is (likely) truncated.
    if len(file_content) >= 79_500:
        return {"error": "file_too_large", "description": (
            "Fichier trop volumineux pour une correction sûre (risque de troncature). "
            "Correction manuelle recommandée."
        )}
    kw = dict(
        file_path=file_path, file_content=file_content, issue_key=issue_key, issue_label=issue_label,
        url=url, site_name=site_name, occurrences_hint=occurrences_hint, model_override=model_override,
    )
    # 1) Targeted edits (cheap: small output). 2) Full-file fallback (reliable) if no edit applied.
    res = _patch_via_edits(**kw)
    if res.get("no_change"):
        return res
    if not res.get("patched_content"):
        res = _patch_via_full_file(**kw)
    if res.get("no_change"):
        return res
    if not (isinstance(res, dict) and res.get("patched_content")):
        return {}
    patched = str(res.get("patched_content") or "")
    # #4 safety: reject patches that drop a large chunk of the file.
    if file_content and len(patched) < int(len(file_content) * 0.5):
        return {"error": "suspicious_patch", "description": (
            "La correction générée supprime une grande partie du fichier — rejetée par sécurité."
        )}
    # #4 safety: keep structured-config files parseable.
    integrity = _validate_patched_file(file_path, patched)
    if integrity is not None:
        return {"error": "invalid_syntax", "description": integrity}
    return res


def _validate_patched_file(file_path: str, patched: str) -> str | None:
    """Return an error message if the patched content breaks the file's syntax, else None."""
    suffix = (file_path or "").rsplit(".", 1)[-1].lower()
    try:
        if suffix == "json" or file_path.endswith((".json",)):
            json.loads(patched)
        elif suffix == "toml":
            try:
                import tomllib  # py3.11+
                tomllib.loads(patched)
            except ModuleNotFoundError:
                pass
    except Exception as e:  # noqa: BLE001 - surface a friendly message to the UI
        return f"Le fichier corrigé n'est plus un {suffix.upper()} valide : {e}"
    return None


def _norm_url_for_match(u: str) -> str:
    """Scheme/trailing-slash-insensitive URL key for matching tasks to report URLs."""
    s = (u or "").strip().lower()
    if not s:
        return ""
    s = s.split("://", 1)[-1]  # drop scheme so http/https compare equal
    s = s.split("#", 1)[0]
    s = s.rstrip("/")
    return s


def _report_issue_counts(report: dict[str, Any]) -> dict[str, int]:
    """Issue key → occurrence count for one crawl report."""
    issues = report.get("issues") if isinstance(report, dict) else None
    if not isinstance(issues, dict):
        return {}
    out: dict[str, int] = {}
    for key, block in issues.items():
        if isinstance(block, dict):
            try:
                out[str(key)] = int(block.get("count") or 0)
            except Exception:
                out[str(key)] = 0
    return out


def _is_delta_metric_key(key: str) -> bool:
    """Between-crawl change metrics ('title tag changed', 'became non-indexable', 'pages added to
    sitemaps'…). They move by construction as soon as anything is fixed, so counting them as
    damage would drown the signal they are meant to support."""
    k = (key or "").lower()
    return (
        "_changed" in k
        or "became_" in k
        or k.startswith(("pages_added", "pages_removed", "no_of_urls"))
    )


def _collateral_introduced(
    before: dict[str, int], after: dict[str, int], *, limit: int = 10
) -> list[dict[str, Any]]:
    """Issues that GREW between the crawl a fix was decided on and the crawl that verified it.

    A corrector that edits code has to answer "what did it break?", not only "did it go away?".
    Two real precedents: fixing title_too_short created title_too_long on 9 pages, and a sitemap
    change cascaded into +4 missing_reciprocal_hreflang."""
    grown: list[dict[str, Any]] = []
    for key, now in after.items():
        if _is_delta_metric_key(key):
            continue
        was = int(before.get(key, 0))
        if now > was:
            grown.append({"key": key, "before": was, "after": int(now), "delta": int(now) - was})
    grown.sort(key=lambda g: (-g["delta"], g["key"]))
    return grown[: max(0, limit)]


def _verify_corrections_after_crawl(slug: str, report: dict[str, Any], runs_dir: Path | None = None) -> None:
    """#5 — After a fresh crawl, confirm whether applied corrections actually worked.

    For each IssueTask that was pushed/applied (status in_progress/done), check if the
    fresh crawl still flags the same (issue_key, url). Records the outcome inside the
    task's `note` JSON (`verify` block) without inventing new status strings (the UI
    buckets unknown statuses as "todo"). A merged ("done") fix that no longer appears is
    confirmed resolved; one that still appears is flagged as a regression in the note.
    """
    try:
        issues = report.get("issues") if isinstance(report.get("issues"), dict) else {}
        if not issues:
            return
        crawl_ts = ""
        meta = report.get("meta") if isinstance(report.get("meta"), dict) else {}
        if isinstance(meta, dict):
            crawl_ts = str(meta.get("timestamp") or meta.get("crawl_ts") or "")
        with DB.session() as db:
            proj = db.scalar(select(Project).where(Project.slug == slug))
            if proj is None:
                return
            tasks = list(db.scalars(select(IssueTask).where(
                IssueTask.project_id == str(proj.id),
                IssueTask.status.in_(["in_progress", "done"]),
            )).all())
            if not tasks:
                return
            impacted_cache: dict[str, set[str]] = {}

            def _impacted_norm(key: str) -> set[str]:
                if key not in impacted_cache:
                    raw = dash.extract_impacted_pages(key, issues.get(key))
                    impacted_cache[key] = {_norm_url_for_match(u) for u in raw}
                return impacted_cache[key]

            # Collateral damage, measured against the crawl each fix was decided on. When several
            # fixes share the same window the delta belongs to the set, not to one PR — recorded
            # as `fixes_in_window` so the UI never blames a single correction for the whole shift.
            after_counts = _report_issue_counts(report)
            window_sizes = Counter(str(t.crawl_ts or "") for t in tasks)
            baseline_cache: dict[str, dict[str, int]] = {}

            def _baseline_counts(ts: str) -> dict[str, int]:
                if ts not in baseline_cache:
                    base: dict[str, int] = {}
                    if ts and runs_dir is not None and ts != crawl_ts:
                        try:
                            old = dash.load_report_json(runs_dir, slug, ts)
                            base = _report_issue_counts(old) if isinstance(old, dict) else {}
                        except Exception:
                            base = {}
                    baseline_cache[ts] = base
                return baseline_cache[ts]

            now_iso = datetime.now(timezone.utc).isoformat()
            changed = False
            for t in tasks:
                key = str(t.issue_key or "")
                if key == _KEYWORD_REWRITE_KEY:
                    # A crawl cannot answer this one. It never flags the key, so the loop below
                    # would read "absent from the report" as "resolved" and badge the task
                    # verified — for a rewrite whose only real verdict is weeks of Search Console
                    # clicks. No reading beats a confident wrong one.
                    continue
                block = issues.get(key)
                count = int(block.get("count") or 0) if isinstance(block, dict) else 0
                url_norm = _norm_url_for_match(str(t.url or ""))
                if count <= 0:
                    still_present = False
                elif url_norm:
                    still_present = url_norm in _impacted_norm(key)
                else:
                    still_present = True  # task without a specific URL: issue still exists
                result = "still_present" if still_present else "resolved"
                # PR opened but not merged + still present = expected, don't flag.
                if t.status == "in_progress" and still_present:
                    continue
                try:
                    note_obj = json.loads(t.note) if t.note else {}
                    if not isinstance(note_obj, dict):
                        note_obj = {"_note": str(t.note)}
                except Exception:
                    note_obj = {"_note": str(t.note)} if t.note else {}
                prev = note_obj.get("verify") if isinstance(note_obj.get("verify"), dict) else {}
                if prev.get("result") == result and prev.get("crawl_ts") == crawl_ts:
                    continue
                _base_ts = str(t.crawl_ts or "")
                _base_counts = _baseline_counts(_base_ts)
                note_obj["verify"] = {
                    "result": result,
                    "verified_at": now_iso,
                    "crawl_ts": crawl_ts,
                }
                # Only claim a collateral reading when the baseline report actually loaded.
                # Diffing against an EMPTY baseline would report every surviving issue as newly
                # introduced — a spectacular false positive on any pruned or unreadable run.
                if _base_counts:
                    note_obj["verify"].update({
                        "baseline_ts": _base_ts,
                        "introduced": _collateral_introduced(_base_counts, after_counts),
                        "fixes_in_window": int(window_sizes.get(_base_ts, 1)),
                    })
                t.note = json.dumps(note_obj, ensure_ascii=False)
                # A confirmed-resolved fix is complete.
                if result == "resolved" and t.status != "done":
                    t.status = "done"
                changed = True
            if changed:
                db.commit()
    except Exception:
        logger.exception("[verify] correction verification failed for slug=%s", slug)


def _github_fixable_issue_candidates(
    *,
    report: dict[str, Any],
    proj,
    limit: int = 8,
) -> list[dict[str, Any]]:
    summary = dash.summarize_report(report)
    issues = summary.get("issues") if isinstance(summary.get("issues"), list) else []
    site_name = str(getattr(proj, "site_name", "") or getattr(proj, "slug", "") or "")
    base_url = str(getattr(proj, "base_url", "") or "")
    priority_order = {"high": 0, "medium": 1, "low": 2}

    candidates: list[dict[str, Any]] = []
    for it in issues:
        if not isinstance(it, dict):
            continue
        issue_key = str(it.get("key") or "").strip()
        if not issue_key or not _github_issue_auto_fixable(issue_key):
            continue
        count = int(it.get("count") or 0)
        if count <= 0:
            continue
        # The redirect-config family is fixable in principle but only for a URL that redirects
        # to ITSELF. Offering "Créer PR" for a site whose redirects are all deliberate
        # canonicalisation would hand the user a button that can only refuse.
        if issue_key in _REDIRECT_CONFIG_KEYS:
            _blk = (report.get("issues") or {}).get(issue_key) if isinstance(report.get("issues"), dict) else None
            if not _redirect_3xx_self_loops(_blk):
                continue
        sample_urls = _issue_sample_urls_from_report(report, issue_key, limit=3)
        primary_url = sample_urls[0] if sample_urls else base_url
        if _validate_settings_url(primary_url):
            primary_url = base_url if not _validate_settings_url(base_url) else ""
        suggestion = fix_suggestions.suggest_issue_fix(
            issue_key=issue_key,
            label=str(it.get("label") or issue_key),
            category=str(it.get("category") or ""),
            severity=str(it.get("severity") or ""),
            count=count,
            report=report,
            site_name=site_name,
            base_url=base_url,
        )
        candidates.append(
            {
                "key": issue_key,
                "label": str(suggestion.get("label") or it.get("label") or issue_key),
                "category": str(suggestion.get("category") or it.get("category") or ""),
                "severity": str(suggestion.get("severity") or it.get("severity") or "notice"),
                "count": count,
                "priority": str(suggestion.get("priority") or "medium"),
                "effort": str(suggestion.get("effort") or "medium"),
                "url": primary_url,
                "sample_urls": sample_urls,
                "candidate_files": _seo_file_candidates_for_issue(issue_key)[:5],
                "why": str(suggestion.get("why") or ""),
            }
        )

    candidates.sort(
        key=lambda x: (
            priority_order.get(str(x.get("priority") or ""), 9),
            dash.SEVERITY_ORDER.get(str(x.get("severity") or "notice"), 99),
            -int(x.get("count") or 0),
            str(x.get("label") or ""),
        )
    )
    return candidates[: max(0, int(limit))]


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

    _prov = _correction_ai_provider()
    model = _correction_ai_model(_prov) or "unknown"
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
    from reportlab.lib.enums import TA_LEFT
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


def _gsc_live_credentials_status(*, user_id: str, slug: str) -> dict[str, str | bool]:
    """
    Lightweight status (no temp files) used by the UI to decide whether live GSC can run.
    """
    oauth_refresh = _gsc_oauth_refresh_token(user_id, slug)
    if oauth_refresh:
        client_id, client_secret = _google_oauth_client()
        if client_id and client_secret:
            return {"ready": True, "auth_mode": "oauth", "reason": ""}
        return {"ready": False, "auth_mode": "oauth", "reason": "oauth_not_configured"}

    env_creds = _resolve_repo_path(_safe_env("GOOGLE_APPLICATION_CREDENTIALS"))
    if env_creds and env_creds.exists():
        return {"ready": True, "auth_mode": "service_account", "reason": ""}
    if _safe_env("GOOGLE_APPLICATION_CREDENTIALS"):
        return {"ready": False, "auth_mode": "service_account", "reason": "credentials_file_not_found"}
    return {"ready": False, "auth_mode": "", "reason": "missing_credentials"}


def _classify_google_oauth_failure(err: Exception) -> str:
    """
    Best-effort classification for Google OAuth refresh failures surfaced by `google-auth`.
    Used to provide cleaner UX than raw exception strings.
    """
    msg = f"{type(err).__name__}: {err}".lower()
    if "invalid_grant" in msg:
        return "oauth_invalid_grant"
    if "invalid_client" in msg:
        return "oauth_invalid_client"
    if "google_oauth_client_id" in msg or "google_oauth_client_secret" in msg:
        return "oauth_not_configured"
    if "oauth credentials are missing" in msg:
        return "oauth_not_configured"
    return ""


def _clear_stale_gsc_oauth(*, user_id: str, slug: str, reason: str) -> None:
    if reason != "oauth_invalid_grant":
        return
    try:
        _gsc_oauth_clear(user_id, slug)
        logger.info("[GSC] cleared stale oauth token user=%s slug=%s", user_id, slug)
    except Exception as exc:
        logger.error("[GSC] clear stale oauth failed user=%s slug=%s: %s: %s", user_id, slug, type(exc).__name__, exc)


def _gsc_oauth_status_hint(reason: str) -> str:
    reason = str(reason or "").strip().lower()
    if reason == "oauth_invalid_grant":
        return "Accès Google révoqué ou expiré. Reconnecte ce projet."
    if reason == "oauth_invalid_client":
        return "OAuth Google invalide côté plateforme. Vérifie le client Google."
    if reason == "oauth_not_configured":
        return "OAuth Google n’est pas configuré côté plateforme."
    if reason == "credentials_file_not_found":
        return "Le fichier de credentials Google n’a pas été trouvé."
    if reason == "missing_credentials":
        return "Aucune connexion Google active pour ce projet."
    return ""


def _google_api_error_info(resp: Any) -> dict[str, str]:
    status = ""
    reason = ""
    message = ""
    try:
        data = resp.json()
    except Exception:
        data = None
    if isinstance(data, dict):
        err = data.get("error")
        if isinstance(err, dict):
            status = str(err.get("status") or "").strip()
            message = str(err.get("message") or "").strip()
            errors = err.get("errors")
            if isinstance(errors, list):
                for item in errors:
                    if not isinstance(item, dict):
                        continue
                    if not message:
                        message = str(item.get("message") or "").strip()
                    reason = str(item.get("reason") or item.get("domain") or "").strip()
                    if reason:
                        break
    if not message:
        message = str(getattr(resp, "text", "") or "").strip()
    if len(message) > 400:
        message = message[:400] + "…"
    return {
        "status": status.lower(),
        "reason": reason.lower(),
        "message": message,
    }


def _classify_gsc_api_failure(resp: Any) -> tuple[str, str]:
    info = _google_api_error_info(resp)
    text = f"{getattr(resp, 'status_code', '')} {info.get('status', '')} {info.get('reason', '')} {info.get('message', '')}".lower()
    if getattr(resp, "status_code", 0) == 401:
        return "gsc_auth_failed", "Reconnecte Google pour ce projet."
    if "accessnotconfigured" in text or "service_disabled" in text or "api has not been used" in text:
        return "gsc_api_disabled", "Active Search Console API dans Google Cloud, puis réessaie."
    if getattr(resp, "status_code", 0) == 429 or "quota" in text or "rate limit" in text or "userratelimitexceeded" in text:
        return "gsc_rate_limited", "Quota Google atteint temporairement. Réessaie plus tard."
    if "insufficient" in text or "forbidden" in text or "permission" in text:
        return "gsc_insufficient_scope", "Reconnecte Google et accepte bien l’accès Search Console."
    return "gsc_request_failed", ""


def _fetch_gsc_live_series(*, user_id: str, slug: str, base_url: str, gsc_cfg: dict[str, Any], days: int) -> dict[str, Any]:
    enabled = bool(gsc_cfg.get("enabled")) if "enabled" in gsc_cfg else True
    if not enabled:
        return {"ok": False, "enabled": False, "source": "gsc", "reason": "disabled"}

    with _gsc_live_credentials(user_id=user_id, slug=slug) as (credentials_path, auth_mode, cred_reason):
        if not credentials_path:
            return {
                "ok": False,
                "enabled": True,
                "source": "gsc",
                "reason": cred_reason or "missing_credentials",
            }

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
                oauth_reason = _classify_google_oauth_failure(e)
                if oauth_reason:
                    _clear_stale_gsc_oauth(user_id=user_id, slug=slug, reason=oauth_reason)
                    return {"ok": False, "enabled": True, "source": "gsc", "reason": oauth_reason}
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
        item = {
            "keyword": key,
            "clicks": _to_int(row.get("clicks")),
            "impressions": _to_int(row.get("impressions")),
            "ctr": _to_float(row.get("ctr")),
            "position": _to_float(row.get("position")),
        }
        # A second key means the request asked for query AND page together — the pairing that
        # turns "this query underperforms" into "this page underperforms on this query", i.e.
        # something the corrector can be pointed at.
        if len(keys) > 1 and str(keys[1] or "").strip():
            item["page"] = str(keys[1]).strip()
        items.append(item)
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

    with _gsc_live_credentials(user_id=user_id, slug=slug) as (credentials_path, auth_mode, cred_reason):
        if not credentials_path:
            return {"ok": False, "enabled": True, "source": "gsc", "reason": cred_reason or "missing_credentials"}

        dimension = (dim or "query").strip().lower()
        if dimension not in {"query", "page", "query_page"}:
            dimension = "query"
        # `fetch_gsc` has always taken a LIST; only this caller narrowed it to one entry, so the
        # query→page pairing Search Console can return was never asked for.
        dimensions = ["query", "page"] if dimension == "query_page" else [dimension]

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
        best_empty: dict[str, Any] | None = None

        for property_url in _gsc_property_candidates(base_url, str(gsc_cfg.get("property_url") or "").strip()):
            try:
                rows = gsc_fetch.fetch_gsc(
                    credentials_path=credentials_path.resolve(),
                    property_url=property_url,
                    start_date=start_date,
                    end_date=end_date,
                    dimensions=dimensions,
                    search_type=search_type,
                    row_limit=fetch_limit,
                    timeout_s=30.0,
                )
            except Exception as e:
                oauth_reason = _classify_google_oauth_failure(e)
                if oauth_reason:
                    _clear_stale_gsc_oauth(user_id=user_id, slug=slug, reason=oauth_reason)
                    return {"ok": False, "enabled": True, "source": "gsc", "reason": oauth_reason}
                last_error = f"{type(e).__name__}: {e}"
                continue

            items = _gsc_rows_to_perf_items(rows if isinstance(rows, list) else [])
            if min_impressions:
                items = [it for it in items if _to_int(it.get("impressions")) >= min_impressions]
            items.sort(key=lambda r: (-_to_int(r.get("clicks")), -_to_int(r.get("impressions"))))
            if limit and limit > 0:
                items = items[: int(limit)]

            result = {
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
            if items:
                return result
            if best_empty is None:
                best_empty = result

        if best_empty is not None:
            return best_empty

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
    scheme = (parts.scheme or "").strip().lower()
    if scheme not in {"http", "https"}:
        return "Schéma non autorisé (http/https uniquement)."
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


class CompatJinja2Templates(Jinja2Templates):
    def TemplateResponse(self, *args: Any, **kwargs: Any) -> Any:
        if args and isinstance(args[0], str):
            name = args[0]
            context = args[1] if len(args) >= 2 else kwargs.pop("context", None)
            if context is None:
                context = {}
            if not isinstance(context, dict):
                return super().TemplateResponse(*args, **kwargs)
            request = kwargs.pop("request", None) or context.get("request")
            if request is None:
                raise ValueError("TemplateResponse context must include request")
            return super().TemplateResponse(request, name, context, *args[2:], **kwargs)
        return super().TemplateResponse(*args, **kwargs)


templates = CompatJinja2Templates(directory=str(REPO_ROOT / "seo-agent-web" / "templates"))


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


def _parse_sites_summary_md(md_path: Path | None) -> list[dict[str, str]]:
    if not md_path or not md_path.exists():
        return []
    try:
        content = md_path.read_text(encoding="utf-8")
    except Exception:
        return []
    rows: list[dict[str, str]] = []
    headers: list[str] = []
    separator_seen = False
    for line in content.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            if headers:
                break
            continue
        cells = [c.strip() for c in stripped.strip("|").split("|")]
        if not headers:
            headers = cells
            continue
        if all(set(c) <= set("-: ") for c in cells):
            separator_seen = True
            continue
        if separator_seen:
            row = {headers[i]: (cells[i] if i < len(cells) else "") for i in range(len(headers))}
            rows.append(row)
    return rows


def _read_inventory_domains(config_path: Path) -> tuple[str | None, str, str, list[str]]:
    """Returns (csv_path_str, delimiter, domain_col, list_of_domains)."""
    try:
        cfg = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except Exception:
        return None, ";", "domain", []
    if not isinstance(cfg, dict):
        return None, ";", "domain", []
    inv = cfg.get("inventory") if isinstance(cfg.get("inventory"), dict) else {}
    domains_csv = (inv or {}).get("domains_csv")
    if not isinstance(domains_csv, str) or not domains_csv.strip():
        return None, ";", "domain", []
    delimiter = str((inv or {}).get("delimiter") or ";")
    preferred_col = (inv or {}).get("domain_column") or "domain"
    csv_path = Path(domains_csv).expanduser()
    if not csv_path.is_absolute():
        csv_path = (config_path.parent / csv_path).resolve()
    if not csv_path.exists():
        return str(csv_path), delimiter, str(preferred_col), []
    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            hdrs = list(reader.fieldnames or [])
            col = hdrs[0] if hdrs else str(preferred_col)
            pn = _norm_header(str(preferred_col))
            for h in hdrs:
                if _norm_header(h) == pn:
                    col = h
                    break
            domains = [str(row.get(col) or "").strip() for row in reader]
            domains = [d for d in domains if d]
        return str(csv_path), delimiter, col, domains
    except Exception:
        return str(csv_path), delimiter, str(preferred_col), []


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
        cfg = _resolve_config_path(job.config_path) if job.config_path else None
        if cfg and not _config_path_allowed(cfg):
            job.status = "failed"
            job.returncode = 2
            job.stderr = (job.stderr or "") + "\n[WORKER] Config path not allowed\n"
            job.finished_at = time.time()
            _save_job(job)
            return
        _run_crawl_job(job.id, user_id, slug, cfg)
        return

    if jtype == "competitor":
        _run_competitor_crawl_job(
            job.id, str(result.get("user_id") or "").strip(),
            str(result.get("competitor_id") or "").strip(),
        )
        return

    if jtype == "autopilot":
        cfg = _resolve_config_path(job.config_path) if job.config_path else None
        if not cfg:
            job.status = "failed"
            job.returncode = 2
            job.stderr = (job.stderr or "") + "\n[WORKER] Missing config_path\n"
            job.finished_at = time.time()
            _save_job(job)
            return
        if not _config_path_allowed(cfg):
            job.status = "failed"
            job.returncode = 2
            job.stderr = (job.stderr or "") + "\n[WORKER] Config path not allowed\n"
            job.finished_at = time.time()
            _save_job(job)
            return
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
            logger.error("[WORKER] claim error: %s: %s", type(e).__name__, e)
            _sentry_capture_exception(e, where="worker.claim_next_job", meta={"worker_id": worker_id})
            _WORKER_STOP.wait(1.0)
            continue

        if not jid:
            _WORKER_STOP.wait(1.0)
            continue

        try:
            _execute_queued_job(jid)
        except Exception as e:
            _sentry_capture_exception(e, where="worker.execute_job", meta={"worker_id": worker_id, "job_id": jid})
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
        logger.error("[RETENTION] jobs cleanup error: %s: %s", type(e).__name__, e)


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
        logger.error("[RETENTION] runs cleanup error: %s: %s", type(e).__name__, e)
        return

    if removed:
        logger.info("[RETENTION] removed runs: %s", removed)


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

        raw_rate = str(os.getenv("SENTRY_TRACES_SAMPLE_RATE", "0.05"))
        try:
            rate = float(raw_rate)
        except Exception:
            rate = 0.05

        # SDK 2.x: FastApiIntegration/StarletteIntegration are auto-detected.
        # Do NOT call app.add_middleware() here — the middleware stack is already
        # frozen at startup time, which raises RuntimeError and corrupts init.
        sentry_sdk.init(
            dsn=dsn,
            traces_sample_rate=max(0.0, min(1.0, rate)),
            environment=str(os.getenv("SENTRY_ENVIRONMENT") or os.getenv("RENDER_SERVICE_NAME") or "prod"),
            release=str(os.getenv("RENDER_GIT_COMMIT") or ""),
        )
        _SENTRY_READY = True
    except Exception as e:
        logger.warning("[SENTRY] init error: %s: %s", type(e).__name__, e)


def _sentry_capture_exception(exc: Exception, *, where: str = "", meta: dict[str, Any] | None = None) -> None:
    if not _SENTRY_READY:
        return
    try:
        import sentry_sdk  # type: ignore

        with sentry_sdk.push_scope() as scope:
            if where:
                scope.set_tag("where", str(where)[:80])
            if isinstance(meta, dict) and meta:
                scope.set_context("meta", meta)
            sentry_sdk.capture_exception(exc)
    except Exception:
        return


def _run_alembic_upgrade_head() -> None:
    web_root = REPO_ROOT / "seo-agent-web"
    alembic_ini = web_root / "alembic.ini"
    if not alembic_ini.exists():
        DB.create_tables()
        return
    proc = subprocess.run(
        [sys.executable, "-m", "alembic", "-c", str(alembic_ini), "upgrade", "head"],
        cwd=str(web_root),
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"alembic upgrade head failed: {detail[:2000]}")


def _validate_startup_config() -> None:
    if not _strict_config_enabled():
        return

    errors: list[str] = []
    public_base = _safe_env("PUBLIC_BASE_URL").rstrip("/")
    session_secret = _safe_env("SEO_AGENT_SECRET_KEY")
    encryption_seed = _current_encryption_seed()
    cron_secret = _safe_env("CRON_SECRET")

    if not _safe_env("DATABASE_URL"):
        errors.append("DATABASE_URL is required when SEO_AGENT_STRICT_CONFIG is enabled")

    if not public_base:
        errors.append("PUBLIC_BASE_URL is required when SEO_AGENT_STRICT_CONFIG is enabled")
    else:
        parsed = urlsplit(public_base)
        host = (parsed.hostname or "").lower()
        if parsed.scheme != "https":
            errors.append("PUBLIC_BASE_URL must use https in strict config")
        if host in {"localhost", "127.0.0.1", "::1"}:
            errors.append("PUBLIC_BASE_URL must not point to localhost in strict config")

    if _weak_secret(session_secret):
        errors.append("SEO_AGENT_SECRET_KEY must be a long random value")

    if _weak_secret(encryption_seed):
        errors.append("SEO_AGENT_ENCRYPTION_KEY or SEO_AGENT_ENCRYPTION_KEYS must be set to a long random value")
    elif session_secret and encryption_seed == session_secret:
        errors.append("SEO_AGENT_ENCRYPTION_KEY must be distinct from SEO_AGENT_SECRET_KEY")

    if _weak_secret(cron_secret):
        errors.append("CRON_SECRET must be set to a long random value")

    if errors:
        raise RuntimeError("Invalid production configuration: " + "; ".join(errors))


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
        logger.error("[BILLING] stale billing reconcile error: %s: %s", type(e).__name__, e)


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

        # Cross-container liveness guard. Web and worker run as SEPARATE Render containers, so
        # _pid_is_alive() is always False for a worker-launched job. But the worker streams
        # stdout/progress to the DB every ~0.6s (bumping updated_at). A recently-updated row means
        # the worker IS alive — typically mid-PageSpeed, which runs for MANY minutes AFTER the
        # "[CRAWL] Done" log line and before report.json is written. Without this, a long crawl
        # (e.g. 19 min with a 17-min PageSpeed phase) trips the done_after_s/stale heuristics below
        # and gets wrongly marked "failed" until a manual refresh re-reads it as done.
        if job.status in {"queued", "running"}:
            _hb_s = float(os.getenv("SEO_AGENT_JOB_ACTIVE_HEARTBEAT_SECONDS", "240"))
            try:
                _fresh = _load_job(job.id)
                _upd = float((_fresh.updated_at if _fresh else None) or job.updated_at or 0.0)
            except Exception:
                _upd = float(job.updated_at or 0.0)
            if _hb_s > 0 and _upd > 0 and (time.time() - _upd) < _hb_s:
                return False

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

        # cancel_requested fast-path — runs BEFORE report/output-dir resolution.
        # The worker SIGKILL completes in <10s, so a job still cancel_requested after
        # cancel_after_s means the worker is gone. Finalize as canceled even when no
        # output dir / report can be located (a job cancelled early may have neither),
        # otherwise the projects list and overview stay stuck on "annulation…" forever.
        if job.status == "cancel_requested":
            _started_at = job.started_at or job.created_at or 0.0
            _age_s = max(0.0, time.time() - float(_started_at))
            _cancel_after_s = float(os.getenv("SEO_AGENT_STALE_CANCEL_SECONDS", "30"))
            if _age_s >= _cancel_after_s:
                job.status = "canceled"
                job.returncode = job.returncode if job.returncode is not None else 130
                job.finished_at = job.finished_at if job.finished_at is not None else time.time()
                if not (job.stderr or "").strip():
                    job.stderr = f"[STALE] Job annulé (worker arrêté, âge={int(_age_s)}s)."
                _finalize_crawl_billing_after_stale(job, actual_pages_crawled=None)
                _save_job(job)
                return True

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
            # NOTE: only reliable when web and worker share a process/container.
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
            # cancel_requested uses a short timeout: the worker SIGKILL completes in <10s, so
            # if it's still cancel_requested after 30s the worker is dead and we can finalize.
            cancel_after_s = float(os.getenv("SEO_AGENT_STALE_CANCEL_SECONDS", "30"))

            # Fast-path: empty dir or crawl completed but no report => likely interrupted.
            if job.status == "cancel_requested" and age_s >= cancel_after_s:
                pass  # fall through to finalize
            elif is_empty_dir and age_s < empty_after_s:
                return False
            elif (progress_done or crawl_done_logged) and age_s < done_after_s:
                return False
            elif (not is_empty_dir) and (not (progress_done or crawl_done_logged)) and age_s < stale_after_s:
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
            # Ensure project_url is present so the frontend can redirect after polling.
            if not job.result.get("project_url"):
                _stale_slug = str(job.result.get("slug") or "").strip()
                _stale_ts = str(job.result.get("timestamp") or "").strip()
                if _stale_slug and _stale_ts:
                    job.result["project_url"] = f"/projects/{_stale_slug}?crawl={_stale_ts}"
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
        "pagespeed_workers": _int_in_range(raw.get("pagespeed_workers"), default=6, min_v=1, max_v=20),
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


# A rival crawl is capped hard, and the cap is a budget decision rather than a technical one:
# worker SLOT-TIME is the scarce resource (2 slots, shared by every customer), and the measured
# cost is ~1.75 s/page. 100 pages is ~3 minutes of a slot with PageSpeed off — which it is here,
# because a competitor's Core Web Vitals are none of our business: this crawl reads titles, H1s
# and URLs and nothing else. Owner's decision, 2026-08-29.
_COMPETITOR_MAX_PAGES = 100
# Each rival is another crawl, so the list is bounded too.
_COMPETITOR_MAX_PER_PROJECT = 5
# A month between automatic refreshes. A rival does not republish its site every week, and the
# customer can always ask for a fresh pass from the page.
_COMPETITOR_REFRESH_DAYS = 30


def _competitor_pages_from_report(report: dict[str, Any]) -> list[dict[str, Any]]:
    """The three fields `competitors.page_terms` reads, and nothing else.

    Deliberately not the whole report: everything else in it describes a site we are not
    auditing — anomalies nobody will fix, and a page count that is not the customer's.
    """
    pages = report.get("pages") if isinstance(report, dict) else None
    out: list[dict[str, Any]] = []
    for page in pages if isinstance(pages, list) else []:
        if not isinstance(page, dict) or page.get("error"):
            continue
        url = str(page.get("url") or "").strip()
        if not url:
            continue
        h1 = page.get("h1")
        out.append({
            "url": url,
            "title": str(page.get("title") or ""),
            "h1": [str(x) for x in h1][:3] if isinstance(h1, list) else str(h1 or ""),
            # The language belongs to the subject: without it a German rival page can be paired
            # with an English page of ours, which is a retargeting PR aimed at the wrong locale.
            "lang": str(page.get("lang") or ""),
            "status_code": page.get("status_code"),
        })
    return out


def _run_competitor_crawl_job(job_id: str, user_id: str, competitor_id: str) -> None:
    """Crawl one rival domain and keep what it says about its own subjects.

    Nothing here touches the customer's project: no report is written, no anomaly is scored, no
    quota is spent. The output is a list of {url, title, h1} stored on the CompetitorSite row.
    """
    import tempfile

    _mark_job_active(job_id, True)
    job = _load_job(job_id)
    if not job:
        _mark_job_active(job_id, False)
        return
    job.status = "running"
    job.started_at = time.time()

    with DB.session() as db:
        row = db.get(CompetitorSite, competitor_id)
        base_url = str(row.base_url or "").strip() if row else ""
        domain = str(row.domain or "") if row else ""
        if row is not None:
            row.status = "crawling"
            row.error = None
            db.commit()

    if not base_url:
        job.status = "failed"
        job.returncode = 2
        job.stderr = f"Concurrent introuvable : {competitor_id}"
        job.finished_at = time.time()
        _save_job(job)
        _mark_job_active(job_id, False)
        return

    validation_err = _validate_public_crawl_target(base_url)
    if validation_err:
        _competitor_mark_failed(competitor_id, f"Cible refusée : {validation_err}")
        job.status = "failed"
        job.returncode = 2
        job.stderr = f"Refus crawl target: {validation_err}"
        job.finished_at = time.time()
        _save_job(job)
        _mark_job_active(job_id, False)
        return

    script = REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py"
    out_dir = Path(tempfile.mkdtemp(prefix="competitor-"))
    cmd = [
        sys.executable, "-u", str(script), base_url,
        "--max-pages", str(_COMPETITOR_MAX_PAGES),
        "--workers", "3",
        "--timeout", "15",
        "--output-dir", str(out_dir),
    ]
    # No --ignore-robots, ever: this is somebody else's site. A rival that refuses us is not a
    # defect to report, it is an answer to respect — the same rule the crawler already follows
    # for a customer's own hosts.
    job.command = cmd
    job.stdout = job.stdout or ""
    job.stderr = job.stderr or ""
    _save_job(job)

    try:
        returncode = _run_subprocess_streaming(
            job, cmd, cwd=REPO_ROOT, job_kind="crawl", timeout_s=1800.0,
        )
        job.returncode = returncode
        report = None
        try:
            report_path = out_dir / "report.json"
            report = json.loads(report_path.read_text(encoding="utf-8")) if report_path.exists() else None
        except Exception:
            report = None
        pages = _competitor_pages_from_report(report) if isinstance(report, dict) else []
        if not pages:
            # Distinguish the two, because they lead to different actions: a site that refused
            # us is not a site we failed to read.
            _competitor_mark_failed(competitor_id, (
                "Aucune page lisible. Ce site bloque peut-être les robots (robots.txt, "
                "pare-feu) : ce n'est pas une anomalie de ton côté."
            ))
            job.status = "failed" if returncode != 0 else "done"
        else:
            with DB.session() as db:
                row = db.get(CompetitorSite, competitor_id)
                if row is not None:
                    row.pages = pages
                    row.pages_count = len(pages)
                    row.status = "ready"
                    row.error = None
                    row.last_crawled_at = datetime.now(timezone.utc)
                    db.commit()
            job.status = "done"
        job.finished_at = time.time()
        _save_job(job)
        logger.info("[competitors] %s: %d page(s) retenues (rc=%s)", domain, len(pages), returncode)
    except Exception as e:
        _competitor_mark_failed(competitor_id, f"{type(e).__name__}: {e}")
        job.status = "failed"
        job.returncode = job.returncode if job.returncode is not None else 1
        job.stderr = _trim_log((job.stderr or "") + f"\n[COMPETITOR] {type(e).__name__}: {e}\n")
        job.finished_at = time.time()
        _save_job(job)
    finally:
        _mark_job_active(job_id, False)
        try:
            import shutil as _shutil
            _shutil.rmtree(out_dir, ignore_errors=True)
        except Exception:
            pass


def _competitor_mark_failed(competitor_id: str, message: str) -> None:
    try:
        with DB.session() as db:
            row = db.get(CompetitorSite, competitor_id)
            if row is not None:
                row.status = "failed"
                row.error = message[:2000]
                row.last_crawled_at = datetime.now(timezone.utc)
                db.commit()
    except Exception:
        pass


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
    gsc_runtime_creds: Path | None = None

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
        "project_url": f"/projects/{slug}?crawl={timestamp}&job={job_id}",
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
    # The PageSpeed quota is the one resource we buy from someone else and cannot buy more of:
    # 25 000 queries/day for the whole platform. Ration it by plan rather than letting a free
    # account spend the same share as a Business one.
    try:
        _plan_ps_urls = int(initial_result.get("max_pagespeed_urls") or 0)
    except (TypeError, ValueError):
        _plan_ps_urls = 0
    if _plan_ps_urls > 0:
        pagespeed_max_urls = min(pagespeed_max_urls, _plan_ps_urls)
    pagespeed_timeout_s = float(crawl_cfg.get("pagespeed_timeout_s") or 60)
    pagespeed_workers = int(crawl_cfg.get("pagespeed_workers") or 6)
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
        # One cache per PROJECT (not per crawl): its whole purpose is to survive from one
        # crawl to the next. The worker has no disk since the horizontal-scaling change, so
        # it has to come back from S3 first and be pushed again after — same lifecycle as a
        # report. A missing cache is a cold start, never an error.
        pagespeed_cache_path = (runs_dir / slug / "pagespeed-cache.json").resolve()
        try:
            _ensure_runs_file_local(pagespeed_cache_path)
        except Exception as e:
            logger.warning("[PAGESPEED] cache restore failed: %s: %s", type(e).__name__, e)
        cmd.extend(["--pagespeed-cache", str(pagespeed_cache_path)])
    if gsc_enabled:
        gsc_dir = site_dir / "gsc"
        gsc_dir.mkdir(parents=True, exist_ok=True)
        cmd.append("--gsc-api")
        # Prefer per-project Google OAuth (refresh token) credentials when available.
        oauth_refresh = _gsc_oauth_refresh_token(str(user_id), slug)
        if oauth_refresh:
            client_id, client_secret = _google_oauth_client()
            if client_id and client_secret:
                try:
                    gsc_runtime_creds = _gsc_write_runtime_oauth_credentials(
                        user_id=str(user_id),
                        slug=slug,
                        refresh_token=oauth_refresh,
                    )
                    cmd.extend(["--gsc-credentials", str(gsc_runtime_creds)])
                except Exception as e:
                    job.stdout = (job.stdout or "") + f"[GSC] credentials error: {type(e).__name__}: {e}\n"
            else:
                job.stdout = (job.stdout or "") + "[GSC] OAuth token présent, mais GOOGLE_OAUTH_CLIENT_ID/SECRET manquants sur ce service.\n"
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
        # Per-plan timeout, resolved at queue time and carried on the job (the worker has no
        # request context). Falls back to the global env default for admin/legacy jobs.
        raw_timeout = str(os.getenv("SEO_AGENT_CRAWL_JOB_TIMEOUT_SECONDS", "21600"))  # 6h
        try:
            plan_timeout = int(initial_result.get("job_timeout_s") or 0)
        except Exception:
            plan_timeout = 0
        if plan_timeout > 0:
            raw_timeout = str(plan_timeout)
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
                # #5 — verify previously applied corrections against this fresh crawl.
                _verify_corrections_after_crawl(slug, report, runs_dir=_runs_dir_for_user(str(user_id)))
            job.result = {
                "type": "crawl",
                "slug": slug,
                "user_id": str(user_id),
                "timestamp": timestamp,
                "project_url": f"/projects/{slug}?crawl={timestamp}&job={job_id}",
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
        if gsc_runtime_creds is not None:
            try:
                gsc_runtime_creds.unlink()
            except Exception:
                pass
        try:
            if site_dir.exists() and site_dir.is_dir():
                _sync_runs_path_to_object_store(site_dir)
        except Exception as e:
            logger.error("[S3] crawl sync error: %s: %s", type(e).__name__, e)
        try:
            # Push the cache even when the crawl failed: the PageSpeed results it collected
            # before dying are still valid, and re-buying them costs the platform's scarcest
            # quota (25 000 API queries/day, shared by every customer).
            ps_cache = (runs_dir / slug / "pagespeed-cache.json").resolve()
            if ps_cache.exists():
                _sync_runs_path_to_object_store(ps_cache)
        except Exception as e:
            logger.error("[S3] pagespeed cache sync error: %s: %s", type(e).__name__, e)
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
                    # A crawl killed by its timeout has still fetched pages and still held a
                    # worker slot for hours. Refunding the whole reservation made retrying an
                    # impossible crawl free for the user and expensive for the platform. Bill
                    # what the crawler reported fetching on stdout, refund the rest — a crawl
                    # that died at page 0 (DNS, 403, cancelled immediately) is still free.
                    prog = job.progress if isinstance(job.progress, dict) else {}
                    try:
                        crawled = max(0, int(prog.get("current") or 0))
                    except Exception:
                        crawled = 0
                    crawled = min(crawled, int(reserved_pages))
                    delta = crawled - int(reserved_pages)
                    if delta != 0:
                        with DB.session() as db:
                            billing.usage_add(
                                db,
                                user_id=str(user_id),
                                metric="pages_crawled_month",
                                amount=int(delta),
                                meta={
                                    "kind": "crawl_partial",
                                    "job_id": job_id,
                                    "slug": slug,
                                    "status": str(job.status),
                                    "reserved_pages": int(reserved_pages),
                                    "pages_crawled_before_failure": int(crawled),
                                },
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
            logger.error("[BILLING] usage update error: %s: %s", type(e).__name__, e)
        _mark_job_active(job_id, False)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

def _startup() -> None:
    _validate_startup_config()
    # Render entrypoint runs Alembic before web/worker startup. Local SQLite needs
    # the same migration path because `create_all()` does not alter stale tables.
    if (not _safe_env("DATABASE_URL")) or _env_bool("SEO_AGENT_DB_AUTO_MIGRATE"):
        _run_alembic_upgrade_head()
    elif _env_bool("SEO_AGENT_DB_AUTO_CREATE"):
        DB.create_tables()
    _init_sentry()
    # Re-apply admin-editable plan overrides from persistent storage into os.environ so they
    # survive restarts (plan_catalog reads PLAN_CONFIG_JSON from the environment).
    try:
        _apply_effective_env("PLAN_CONFIG_JSON")
    except Exception:
        pass
    _start_job_worker()
    _start_retention()


def _shutdown() -> None:
    _WORKER_STOP.set()


@asynccontextmanager
async def _app_lifespan(_app: FastAPI):
    _startup()
    try:
        yield
    finally:
        _shutdown()


# `/docs` is the customer documentation, so FastAPI's interactive schema moves out of the way.
# It also moves BEHIND authentication: the public-path allowlist matches the /docs prefix, and
# leaving Swagger there would have published every internal route to anyone who asked.
app = FastAPI(
    title="SEO Agent",
    lifespan=_app_lifespan,
    docs_url="/internal/api-explorer",
    redoc_url=None,
    openapi_url="/internal/openapi.json",
)
app.mount("/static", StaticFiles(directory=str(REPO_ROOT / "seo-agent-web" / "static")), name="static")


@app.api_route("/favicon.ico", methods=["GET", "HEAD"], include_in_schema=False)
def favicon_ico() -> RedirectResponse:
    return RedirectResponse(url="/static/favicon.ico?v=2", status_code=308)


@app.api_route("/apple-touch-icon.png", methods=["GET", "HEAD"], include_in_schema=False)
def apple_touch_icon() -> RedirectResponse:
    return RedirectResponse(url="/static/apple-touch-icon.png?v=2", status_code=308)


@app.api_route("/site.webmanifest", methods=["GET", "HEAD"], include_in_schema=False)
def site_webmanifest() -> RedirectResponse:
    return RedirectResponse(url="/static/site.webmanifest?v=2", status_code=308)


@app.middleware("http")
async def cors_middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
    if request.method == "OPTIONS":
        origin = request.headers.get("origin", "")
        allowed_origin = str(os.environ.get("PUBLIC_BASE_URL") or "").rstrip("/")
        if origin and allowed_origin and origin == allowed_origin:
            return Response(
                status_code=204,
                headers={
                    "Access-Control-Allow-Origin": origin,
                    "Access-Control-Allow-Credentials": "true",
                    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                    "Access-Control-Allow-Headers": "Content-Type, x-csrf-token, X-CSRF-Token",
                },
            )
        return Response(status_code=403)
    response = await call_next(request)
    origin = request.headers.get("origin", "")
    allowed_origin = str(os.environ.get("PUBLIC_BASE_URL") or "").rstrip("/")
    if origin and allowed_origin and origin == allowed_origin:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Credentials"] = "true"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, x-csrf-token, X-CSRF-Token"
    return response


@app.middleware("http")
async def security_headers_middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
    response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
    if _csp_enabled():
        csp_header = "Content-Security-Policy-Report-Only" if _csp_report_only() else "Content-Security-Policy"
        response.headers.setdefault(csp_header, _content_security_policy(request))
    # HSTS only when served over HTTPS
    if _request_is_secure(request):
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
    return response


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
        "/favicon.ico",
        "/apple-touch-icon.png",
        "/site.webmanifest",
        "/robots.txt",
        "/sitemap.xml",
        "/stripe/webhook",
        "/",
        "/pricing",
        "/terms",
        "/privacy",
        "/support",
        "/status",
    } or path.startswith("/ressources-seo") or path.startswith("/docs"):
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


def _normalize_email(value: str) -> str:
    return str(value or "").strip().lower()


def _safe_next_path(next_path: str | None) -> str:
    n = str(next_path or "").strip()
    if not n:
        return "/"
    if any(ch in n for ch in ("\r", "\n", "\t", "\\")):
        return "/"
    if not n.startswith("/"):
        return "/"
    if n.startswith("//"):
        return "/"
    parts = urlsplit(n)
    if parts.scheme or parts.netloc:
        return "/"
    return n


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
    """The public header. Order matters: it is the order a visitor reads.

    `primary` marks the links the header shows; the footer renders the whole list. Before
    this, /docs and /ressources-seo existed but appeared in neither, reachable only from a
    block halfway down the home page.
    """
    return [
        {"href": "/pricing", "label": "Tarifs", "primary": "1"},
        {"href": "/docs", "label": "Documentation", "primary": "1"},
        {"href": "/ressources-seo", "label": "Guides SEO", "primary": "1"},
        {"href": "/support", "label": "Support", "primary": "1"},
        {"href": "/status", "label": "Statut", "primary": ""},
        {"href": "/terms", "label": "CGU", "primary": ""},
        {"href": "/privacy", "label": "Confidentialité", "primary": ""},
    ]


def _plan_token_value(catalog: dict[str, Any], plan_key: str, group: str, field: str) -> str:
    """One plan number, formatted for prose. Never raises — a missing key renders as "-"."""
    plan = catalog.get(plan_key) if isinstance(catalog.get(plan_key), dict) else {}
    bucket = plan.get(group) if isinstance(plan.get(group), dict) else {}
    raw = bucket.get(field)
    if raw is None:
        return "-"
    try:
        number = int(raw)
    except (TypeError, ValueError):
        return str(raw)
    if number == 0:
        return "0"
    # Narrow no-break space: the French thousands separator that never wraps mid-number.
    return f"{number:,}".replace(",", " ")


def _content_tokens() -> dict[str, str]:
    """Values a content page may interpolate with `{{token}}`.

    Documentation that hardcodes a quota is documentation that lies the first time an admin
    tunes PLAN_CONFIG_JSON. These are read live, per request, from the same catalogue the
    billing code enforces.
    """
    tokens: dict[str, str] = {
        "app_name": _app_name(),
        "support_email": _support_email(),
        "competitors_max": str(_COMPETITOR_MAX_PER_PROJECT),
        "competitor_pages": str(_COMPETITOR_MAX_PAGES),
        "competitor_refresh_days": str(_COMPETITOR_REFRESH_DAYS),
    }
    try:
        catalog = billing.plan_catalog()
    except Exception:
        logger.warning("[content] plan catalogue unavailable; plan tokens will render as-is")
        return tokens
    for key in ("free", "solo", "pro", "business"):
        plan = catalog.get(key) if isinstance(catalog.get(key), dict) else {}
        tokens[f"price_{key}"] = str(plan.get("price_label") or "-")
        tokens[f"label_{key}"] = str(plan.get("label") or key.title())
        tokens[f"projects_{key}"] = _plan_token_value(catalog, key, "limits", "projects")
        tokens[f"pages_{key}"] = _plan_token_value(catalog, key, "limits", "pages_crawled_month")
        tokens[f"corrections_{key}"] = _plan_token_value(catalog, key, "limits", "ai_corrections_month")
        tokens[f"assistant_{key}"] = _plan_token_value(catalog, key, "limits", "assistant_messages_month")
        tokens[f"maxpages_{key}"] = _plan_token_value(catalog, key, "crawl", "max_pages_per_crawl")
        tokens[f"pagespeed_{key}"] = _plan_token_value(catalog, key, "crawl", "max_pagespeed_urls")
        tokens[f"files_{key}"] = _plan_token_value(catalog, key, "correction", "max_files")
    return tokens


def _public_url(request: Request, path: str) -> str:
    clean_path = str(path or "/").strip()
    if not clean_path.startswith("/"):
        clean_path = f"/{clean_path}"
    return f"{_public_base_url(request)}{clean_path}"


def _public_template_context(request: Request, **extra: Any) -> dict[str, Any]:
    ctx: dict[str, Any] = {
        "request": request,
        "app_name": _app_name(),
        "support_email": _support_email(),
        "year": datetime.now(timezone.utc).year,
        "nav_items": _public_nav_items(),
    }
    ctx.update(extra)
    return ctx


def _legal_version() -> str:
    return _safe_env("LEGAL_VERSION") or "0.1"


def _legal_updated_at() -> str:
    return _safe_env("LEGAL_UPDATED_AT") or datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _smtp_send_email(*, to_addr: str, subject: str, body: str, html_body: str = "") -> None:
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
    if html_body:
        msg.add_alternative(str(html_body), subtype="html")

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
    *, api_key: str, to_addr: str, subject: str, body: str, from_addr: str, from_name: str = "", html_body: str = ""
) -> None:
    """SendGrid transactional API."""
    key = str(api_key or "").strip()
    if not key:
        raise RuntimeError("sendgrid_api_key_missing")

    from_obj: dict[str, str] = {"email": str(from_addr).strip()}
    if str(from_name or "").strip():
        from_obj["name"] = str(from_name).strip()

    content_blocks: list[dict[str, str]] = [{"type": "text/plain", "value": str(body)}]
    if html_body:
        content_blocks.append({"type": "text/html", "value": str(html_body)})

    _http_mail_post(
        provider="sendgrid",
        url="https://api.sendgrid.com/v3/mail/send",
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        payload={
            "personalizations": [{"to": [{"email": str(to_addr).strip()}]}],
            "from": from_obj,
            "subject": str(subject),
            "content": content_blocks,
        },
        to_addr=to_addr,
        from_addr=from_addr,
    )


def _brevo_send_email(
    *, api_key: str, to_addr: str, subject: str, body: str, from_addr: str, from_name: str = "", html_body: str = ""
) -> None:
    """Brevo transactional API. 300 emails/day free, EU-hosted."""
    key = str(api_key or "").strip()
    if not key:
        raise RuntimeError("brevo_api_key_missing")

    sender: dict[str, str] = {"email": str(from_addr).strip()}
    if str(from_name or "").strip():
        sender["name"] = str(from_name).strip()
    payload: dict[str, Any] = {
        "sender": sender,
        "to": [{"email": str(to_addr).strip()}],
        "subject": str(subject),
        "textContent": str(body),
    }
    if html_body:
        payload["htmlContent"] = str(html_body)

    _http_mail_post(
        provider="brevo",
        url="https://api.brevo.com/v3/smtp/email",
        headers={"api-key": key, "Content-Type": "application/json", "accept": "application/json"},
        payload=payload,
        to_addr=to_addr,
        from_addr=from_addr,
    )


def _resend_send_email(
    *, api_key: str, to_addr: str, subject: str, body: str, from_addr: str, from_name: str = "", html_body: str = ""
) -> None:
    """Resend transactional API."""
    key = str(api_key or "").strip()
    if not key:
        raise RuntimeError("resend_api_key_missing")

    name = str(from_name or "").strip()
    sender = f"{name} <{str(from_addr).strip()}>" if name else str(from_addr).strip()
    payload: dict[str, Any] = {
        "from": sender,
        "to": [str(to_addr).strip()],
        "subject": str(subject),
        "text": str(body),
    }
    if html_body:
        payload["html"] = str(html_body)

    _http_mail_post(
        provider="resend",
        url="https://api.resend.com/emails",
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        payload=payload,
        to_addr=to_addr,
        from_addr=from_addr,
    )


def _http_mail_post(
    *, provider: str, url: str, headers: dict[str, str], payload: dict[str, Any], to_addr: str, from_addr: str
) -> None:
    """One place where an HTTP mail API is called, logged and judged.

    The provider's own words are what make a refusal actionable — "Maximum credits exceeded"
    took an afternoon to find because the signup handler replaced it with "réessaie plus tard".
    So the response body is logged verbatim (truncated), on every provider, always.
    """
    to_masked = _mask_email(to_addr)
    from_masked = _mask_email(from_addr)
    try:
        print(f"[MAIL] {provider} api sending to={to_masked} from={from_masked}", flush=True)
        resp = requests.post(url, headers=headers, json=payload, timeout=15.0)
    except Exception as e:
        print(f"[MAIL] {provider} api error: {type(e).__name__}: {e}", flush=True)
        raise

    if resp.status_code >= 400:
        detail = (resp.text or "").strip().replace(chr(10), " ")[:500]
        print(f"[MAIL] {provider} api failed status={resp.status_code} detail={detail}", flush=True)
        raise RuntimeError(f"{provider}_api_http_{resp.status_code}")

    # Accepted is not delivered. The provider's own message id is the only way to find this
    # message in THEIR logs, which is where the answer lives once we know we handed it over.
    message_id = ""
    try:
        data = resp.json()
        if isinstance(data, dict):
            for field in ("messageId", "message_id", "id"):
                value = data.get(field)
                if isinstance(value, str) and value.strip():
                    message_id = value.strip()
                    break
    except Exception:
        pass
    if not message_id:
        # SendGrid returns an empty body and puts the id in a header. Guarded like the body
        # above: the mail has already been accepted, and nothing about LOGGING it may turn a
        # successful send into a failed one.
        try:
            message_id = str((resp.headers or {}).get("X-Message-Id") or "").strip()
        except Exception:
            message_id = ""
    print(
        f"[MAIL] {provider} api accepted status={resp.status_code} to={to_masked}"
        + (f" message_id={message_id}" if message_id else " (aucun identifiant renvoyé)"),
        flush=True,
    )


# Which HTTP API a given SMTP host implies. Keyed on host so switching provider stays an
# environment change: set SMTP_HOST/SMTP_USERNAME/SMTP_PASSWORD and the right transport is
# picked automatically, no deploy and no code edit.
_MAIL_API_HOSTS: dict[str, str] = {
    "smtp.sendgrid.net": "sendgrid",
    "smtp-relay.brevo.com": "brevo",
    "smtp-relay.sendinblue.com": "brevo",
    "smtp.resend.com": "resend",
}


def _mail_config() -> dict[str, Any] | None:
    """Everything needed to send, from EITHER an SMTP setup or an HTTP-API-only setup.

    Driving a provider purely over HTTP is the point of MAIL_API_PROVIDER — Brevo's SMTP
    password is not its transactional API key, so configuring it as an SMTP server is a lie.
    But `_smtp_config()` returns None without SMTP_HOST, and `_email_verification_enabled()`
    is built on it: an operator who set the API pair and removed SMTP_HOST would silently turn
    email verification OFF and let every signup through unverified, with no error anywhere.
    Sending would still be configured; only the switch would think otherwise.

    So configuration means "we can send", by either route.
    """
    cfg = _smtp_config()
    if cfg:
        return cfg

    provider = str(_safe_env("MAIL_API_PROVIDER") or "").strip().lower()
    api_key = str(_safe_env("MAIL_API_KEY") or "").strip()
    from_addr = str(_safe_env("MAIL_FROM") or _safe_env("SMTP_FROM") or "").strip()
    if not (provider and api_key and from_addr):
        return None
    return {
        "host": "",
        "port": 0,
        "username": "",
        "password": "",
        "from": from_addr,
        "from_name": str(
            _safe_env("MAIL_FROM_NAME") or _safe_env("SMTP_FROM_NAME") or _safe_env("APP_NAME") or ""
        ).strip(),
        "ssl": False,
        "starttls": False,
        "timeout_s": 10.0,
    }


def _mail_api_transport(cfg: dict[str, Any]) -> tuple[str, str]:
    """(provider, api_key) for the configured host, or ("", "") to use plain SMTP.

    An explicit MAIL_API_PROVIDER + MAIL_API_KEY wins, so a provider can be used over HTTP
    without pretending to be an SMTP server at all.
    """
    explicit = str(_safe_env("MAIL_API_PROVIDER") or "").strip().lower()
    explicit_key = str(_safe_env("MAIL_API_KEY") or "").strip()
    if explicit and explicit_key:
        return explicit, explicit_key

    host = str(cfg.get("host") or "").strip().lower()
    provider = _MAIL_API_HOSTS.get(host, "")
    if not provider:
        return "", ""

    password = str(cfg.get("password") or "").strip()
    username = str(cfg.get("username") or "").strip().lower()
    if provider == "sendgrid":
        # SendGrid's SMTP username is the literal word "apikey"; anything else means the
        # password is not the API key and the HTTP call would 401.
        return ("sendgrid", password) if username == "apikey" and password else ("", "")
    # Brevo's SMTP password is an SMTP key, NOT the transactional API key, so it cannot be
    # reused over HTTP. Resend's SMTP password IS the API key.
    if provider == "resend":
        return ("resend", password) if password else ("", "")
    return "", ""


_MAIL_API_SENDERS = {
    "sendgrid": lambda **kw: _sendgrid_send_email(**kw),
    "brevo": lambda **kw: _brevo_send_email(**kw),
    "resend": lambda **kw: _resend_send_email(**kw),
}


def _send_email(*, to_addr: str, subject: str, body: str, html_body: str = "") -> None:
    """
    Prefer a provider's HTTP API over raw SMTP whenever the configuration identifies one.

    Render and other PaaS platforms commonly filter outbound SMTP ports, so HTTPS is both more
    reliable and the only route that reports a refusal in the provider's own words.
    """
    cfg = _mail_config()
    if not cfg:
        raise RuntimeError("mail_not_configured")
    provider, api_key = _mail_api_transport(cfg)
    # Only name the SMTP host when SMTP is what will actually be used. The dispatch line
    # printed `host=smtp.sendgrid.net` while sending through Brevo, which is exactly the kind
    # of detail that sends a future debugging session to the wrong vendor.
    where = f"{cfg.get('host')}:{cfg.get('port')}" if not provider else provider
    print(
        f"[MAIL] dispatch via={where} transport={provider or 'smtp'} "
        f"from={_mask_email(str(cfg.get('from') or ''))} to={_mask_email(to_addr)}",
        flush=True,
    )
    sender = _MAIL_API_SENDERS.get(provider) if provider else None
    if sender is not None:
        sender(
            api_key=api_key,
            to_addr=to_addr,
            subject=subject,
            body=body,
            from_addr=str(cfg.get("from") or ""),
            from_name=str(cfg.get("from_name") or ""),
            html_body=html_body,
        )
        return
    if provider:
        raise RuntimeError(f"mail_api_provider_unknown_{provider}")

    _smtp_send_email(to_addr=to_addr, subject=subject, body=body, html_body=html_body)


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


def _utc_now_naive() -> datetime:
    """UTC now, tz-stripped, matching what _dt_as_naive_utc returns for stored values.

    Every datetime bug in this file has been the same one: a value normalised to naive being
    compared or subtracted against an aware `datetime.now(timezone.utc)`. Python raises rather
    than guessing, and the raise lands inside a request handler as a 500 or a swallowed
    "réessaie plus tard". Both sides now come from helpers that agree.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _dt_is_past(value: datetime | None) -> bool:
    """True when `value` is already past. Missing means "no expiry", never "expired"."""
    moment = _dt_as_naive_utc(value)
    if moment is None:
        return False
    return moment <= _utc_now_naive()


def _seconds_until(expires_at: datetime | None, *, minimum: int = 60) -> int:
    """Seconds from now until `expires_at`, whether it arrives naive or timezone-aware.

    Both auth emails used to compute this inline as `_dt_as_naive_utc(x) - datetime.now(utc)`,
    i.e. NAIVE minus AWARE, which raises TypeError in Python. It raised on the first line of
    composing the message, before any mail code ran — so email verification and password reset
    had never once worked in production, and the only symptom either produced was a generic
    "réessaie plus tard" with no [MAIL] line in the logs to contradict it.

    One helper, used by both, so the two cannot drift apart again.
    """
    exp = _dt_as_naive_utc(expires_at)
    if exp is None:
        return minimum
    return max(minimum, int((exp - _utc_now_naive()).total_seconds()))


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
    now = datetime.now(timezone.utc)
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
    except Exception as _e:
        logger.warning("[AUTH] failed to invalidate old reset tokens for user %s: %s", uid, _e)

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
    if _dt_is_past(getattr(row, "expires_at", None)):
        return None
    return row


def _send_password_reset_email(*, to_email: str, reset_url: str, expires_at: datetime) -> None:
    logger.info("[MAIL] reset compose to=%s url_host=%s", _mask_email(to_email), urlsplit(str(reset_url)).netloc)
    ttl_s = _seconds_until(expires_at)
    ttl_minutes = max(1, int(math.ceil(float(ttl_s) / 60.0)))

    app_name = _safe_env("APP_NAME") or "SEO Agent"
    subject_tpl = _safe_env("PASSWORD_RESET_EMAIL_SUBJECT")
    if subject_tpl:
        subject = subject_tpl.replace("{app}", app_name).replace("{brand}", app_name).strip()
    else:
        subject = f"Réinitialisation du mot de passe — {app_name}"
    if not subject:
        subject = f"Réinitialisation du mot de passe — {app_name}"

    safe_url = html.escape(str(reset_url).strip(), quote=True)
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
    html_body = f"""<!DOCTYPE html>
<html lang="fr"><head><meta charset="utf-8"><title>{html.escape(subject)}</title></head>
<body style="margin:0;padding:0;background:#f4f4f7;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f4f7;padding:32px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.08);">
        <tr><td style="background:#111;padding:24px 32px;">
          <span style="color:#fff;font-size:18px;font-weight:700;">{html.escape(app_name)}</span>
        </td></tr>
        <tr><td style="padding:32px;">
          <h1 style="font-size:20px;margin:0 0 16px;">Réinitialisation du mot de passe</h1>
          <p style="color:#555;margin:0 0 24px;">Clique sur le bouton ci-dessous pour définir un nouveau mot de passe. Ce lien est valable <strong>{ttl_minutes}&nbsp;minutes</strong>.</p>
          <a href="{safe_url}" style="display:inline-block;background:#111;color:#fff;text-decoration:none;padding:12px 28px;border-radius:6px;font-weight:600;">Réinitialiser mon mot de passe</a>
          <p style="color:#999;font-size:12px;margin:24px 0 0;">Si le bouton ne fonctionne pas, copie ce lien dans ton navigateur :<br><a href="{safe_url}" style="color:#555;word-break:break-all;">{safe_url}</a></p>
          <hr style="border:none;border-top:1px solid #eee;margin:24px 0;">
          <p style="color:#bbb;font-size:12px;margin:0;">Si tu n’es pas à l’origine de cette demande, ignore cet email. Ton mot de passe ne changera pas.</p>
        </td></tr>
        <tr><td style="background:#f4f4f7;padding:16px 32px;text-align:center;">
          <span style="color:#bbb;font-size:12px;">© {datetime.now(timezone.utc).year} {html.escape(app_name)}</span>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body></html>"""
    _send_email(to_addr=str(to_email).strip(), subject=subject, body=body, html_body=html_body)


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
    return bool(_mail_config())


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
    now = datetime.now(timezone.utc)
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
    now = datetime.now(timezone.utc)
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
    if _dt_is_past(getattr(row, "expires_at", None)):
        return None
    return row


def _send_email_verification_email(*, to_email: str, verify_url: str, expires_at: datetime) -> None:
    ttl_s = _seconds_until(expires_at)
    ttl_hours = max(1, int(math.ceil(float(ttl_s) / 3600.0)))

    app_name = _safe_env("APP_NAME") or "SEO Agent"
    subject_tpl = _safe_env("EMAIL_VERIFY_EMAIL_SUBJECT")
    if subject_tpl:
        subject = subject_tpl.replace("{app}", app_name).replace("{brand}", app_name).strip()
    else:
        subject = f"Vérifie ton email — {app_name}"
    if not subject:
        subject = f"Vérifie ton email — {app_name}"

    safe_url = html.escape(str(verify_url).strip(), quote=True)
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
    html_body = f"""<!DOCTYPE html>
<html lang="fr"><head><meta charset="utf-8"><title>{html.escape(subject)}</title></head>
<body style="margin:0;padding:0;background:#f4f4f7;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f4f7;padding:32px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.08);">
        <tr><td style="background:#111;padding:24px 32px;">
          <span style="color:#fff;font-size:18px;font-weight:700;">{html.escape(app_name)}</span>
        </td></tr>
        <tr><td style="padding:32px;">
          <h1 style="font-size:20px;margin:0 0 16px;">Confirme ton adresse email</h1>
          <p style="color:#555;margin:0 0 24px;">Clique sur le bouton ci-dessous pour vérifier ton adresse email. Ce lien est valable <strong>{ttl_hours}&nbsp;h</strong>.</p>
          <a href="{safe_url}" style="display:inline-block;background:#111;color:#fff;text-decoration:none;padding:12px 28px;border-radius:6px;font-weight:600;">Confirmer mon email</a>
          <p style="color:#999;font-size:12px;margin:24px 0 0;">Si le bouton ne fonctionne pas, copie ce lien dans ton navigateur :<br><a href="{safe_url}" style="color:#555;word-break:break-all;">{safe_url}</a></p>
          <hr style="border:none;border-top:1px solid #eee;margin:24px 0;">
          <p style="color:#bbb;font-size:12px;margin:0;">Si tu n’es pas à l’origine de cette demande, ignore cet email.</p>
        </td></tr>
        <tr><td style="background:#f4f4f7;padding:16px 32px;text-align:center;">
          <span style="color:#bbb;font-size:12px;">© {datetime.now(timezone.utc).year} {html.escape(app_name)}</span>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body></html>"""
    _send_email(to_addr=str(to_email).strip(), subject=subject, body=body, html_body=html_body)


def _send_welcome_email(*, to_email: str, dashboard_url: str) -> None:
    app_name = _safe_env("APP_NAME") or "SEO Agent"
    subject = f"Bienvenue sur {app_name} !"
    safe_url = html.escape(str(dashboard_url).strip(), quote=True)
    body = "\n".join(
        [
            f"Bienvenue sur {app_name} !",
            "",
            "Ton compte est prêt. Connecte-toi pour commencer ton audit SEO.",
            str(dashboard_url).strip(),
            "",
            f"— L'équipe {app_name}",
        ]
    )
    html_body = f"""<!DOCTYPE html>
<html lang="fr"><head><meta charset="utf-8"><title>{html.escape(subject)}</title></head>
<body style="margin:0;padding:0;background:#f4f4f7;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f4f7;padding:32px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.08);">
        <tr><td style="background:#111;padding:24px 32px;">
          <span style="color:#fff;font-size:18px;font-weight:700;">{html.escape(app_name)}</span>
        </td></tr>
        <tr><td style="padding:32px;">
          <h1 style="font-size:20px;margin:0 0 16px;">Bienvenue sur {html.escape(app_name)} !</h1>
          <p style="color:#555;margin:0 0 24px;">Ton compte est créé et prêt à l'emploi. Commence par ajouter ton premier site pour lancer un audit SEO complet.</p>
          <a href="{safe_url}" style="display:inline-block;background:#111;color:#fff;text-decoration:none;padding:12px 28px;border-radius:6px;font-weight:600;">Accéder au tableau de bord</a>
          <hr style="border:none;border-top:1px solid #eee;margin:24px 0;">
          <p style="color:#bbb;font-size:12px;margin:0;">Tu reçois cet email car tu viens de créer un compte sur {html.escape(app_name)}.</p>
        </td></tr>
        <tr><td style="background:#f4f4f7;padding:16px 32px;text-align:center;">
          <span style="color:#bbb;font-size:12px;">© {datetime.now(timezone.utc).year} {html.escape(app_name)}</span>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body></html>"""
    try:
        _send_email(to_addr=str(to_email).strip(), subject=subject, body=body, html_body=html_body)
    except Exception as exc:
        logger.warning("[MAIL] welcome email failed for %s: %s", _mask_email(to_email), exc)


def _request_is_secure(request: Request) -> bool:
    forwarded_proto = request.headers.get("x-forwarded-proto") if _trust_proxy_headers() else ""
    proto = (forwarded_proto or request.url.scheme or "http").split(",")[0].strip().lower()
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
    max_bytes = _csrf_body_max_bytes()
    content_length = str(request.headers.get("content-length") or "").strip()
    if content_length:
        try:
            if int(content_length) > max_bytes:
                raise HTTPException(status_code=413, detail="Request body too large")
        except HTTPException:
            raise
        except Exception:
            pass

    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > max_bytes:
            raise HTTPException(status_code=413, detail="Request body too large")
        chunks.append(chunk)
    body = b"".join(chunks)
    request._body = body  # type: ignore[attr-defined]
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


def _valid_ip_string(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        return str(ipaddress.ip_address(raw))
    except Exception:
        return ""


def _request_client_ip(request: Request) -> str:
    candidates: list[str] = []
    if _trust_proxy_headers():
        xff = str(request.headers.get("x-forwarded-for") or "").strip()
        if xff:
            candidates.extend(part.strip() for part in xff.split(","))
        xri = str(request.headers.get("x-real-ip") or "").strip()
        if xri:
            candidates.append(xri)
    if request.client and request.client.host:
        candidates.append(str(request.client.host))
    for raw in candidates:
        ip = _valid_ip_string(raw)
        if ip:
            return ip
    return ""


def _rate_limit_retry_after_memory(*, bucket: str, subject: str, limit: int, window_s: int) -> int | None:
    """In-memory fallback — used when DB is unavailable."""
    key = f"{bucket}:{subject}"
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
            return max(1, int(math.ceil(float(window_s) - (now - queue[0]))))
        queue.append(now)
    return None


def _rate_limit_retry_after(*, bucket: str, subject: str, limit: int, window_s: int) -> int | None:
    normalized_bucket = str(bucket or "").strip()
    normalized_subject = str(subject or "").strip()
    if not normalized_bucket or not normalized_subject or limit <= 0 or window_s <= 0:
        return None
    key = f"{normalized_bucket}:{normalized_subject}"
    now = time.time()
    cutoff = now - float(window_s)
    try:
        with DB.session() as db:
            row = db.execute(
                select(RateLimitBucket)
                .where(RateLimitBucket.key == key)
                .with_for_update()
            ).scalar_one_or_none()
            hits: list[float] = []
            if row is not None:
                try:
                    raw = json.loads(row.hits_json or "[]")
                    hits = [float(h) for h in raw if isinstance(h, (int, float))]
                except Exception:
                    hits = []
            hits = [h for h in hits if h > cutoff]
            if len(hits) >= int(limit):
                retry_after = max(1, int(math.ceil(float(window_s) - (now - hits[0]))))
                # Persist pruned list even when rate-limited
                if row is not None:
                    row.hits_json = json.dumps(hits)
                    db.add(row)
                    db.commit()
                return retry_after
            hits.append(now)
            if row is None:
                db.add(RateLimitBucket(key=key, hits_json=json.dumps(hits)))
            else:
                row.hits_json = json.dumps(hits)
                db.add(row)
            db.commit()
        return None
    except Exception as _e:
        logger.warning("[RL] DB rate limit unavailable, using in-memory fallback: %s", _e)
        return _rate_limit_retry_after_memory(
            bucket=normalized_bucket, subject=normalized_subject, limit=limit, window_s=window_s
        )


def _format_retry_after(retry_after_s: int) -> str:
    retry_after = max(1, int(retry_after_s))
    if retry_after >= 60:
        minutes = int(math.ceil(retry_after / 60.0))
        return f"{minutes} min"
    return f"{retry_after}s"


def _fmt_duration(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s"
    m, s = divmod(seconds, 60)
    if m < 60:
        return f"{m}m{s:02d}s" if s else f"{m}min"
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m"


def _crawl_timing_map(slug: str) -> dict[str, dict[str, Any]]:
    timing: dict[str, dict[str, Any]] = {}
    for j in _list_jobs(limit=500):
        r = j.result if isinstance(j.result, dict) else {}
        if r.get("type") != "crawl" or str(r.get("slug") or "").strip() != slug:
            continue
        ts = str(r.get("timestamp") or "").strip()
        if not ts:
            continue
        sa = float(j.started_at or j.created_at or 0)
        fa = float(j.finished_at or 0)
        dur_s = int(fa - sa) if fa > sa else None
        timing[ts] = {
            "duration_s": dur_s,
            "duration_label": _fmt_duration(dur_s) if dur_s is not None else None,
        }
    return timing


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
        "/favicon.ico",
        "/apple-touch-icon.png",
        "/site.webmanifest",
        "/robots.txt",
        "/sitemap.xml",
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
        "/cron/check-backlinks",
        "/cron/autopilot",
        "/cron/auto-search-backlinks",
        "/cron/auto-post-backlinks",
    } or path.startswith("/ressources-seo") or path.startswith("/docs"):
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
        "group": "Sécurité",
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
    "SEO_AGENT_ENCRYPTION_KEY": {
        "label": "Clé de chiffrement",
        "hint": "Secret dédié au chiffrement des connexions",
        "group": "Sécurité",
        "order": 45,
        "editable": True,
        "help": {
            "title": "SEO_AGENT_ENCRYPTION_KEY",
            "steps": [
                "Secret dédié au chiffrement des tokens stockés en base.",
                "Doit être long, aléatoire, et différent de SEO_AGENT_SECRET_KEY.",
                "Pour une rotation, préfère SEO_AGENT_ENCRYPTION_KEYS avec la nouvelle clé en premier.",
            ],
        },
    },
    "SEO_AGENT_ENCRYPTION_KEYS": {
        "label": "Clés de chiffrement (rotation)",
        "hint": "Nouvelle clé en premier, anciennes ensuite",
        "group": "Sécurité",
        "order": 46,
        "editable": True,
    },
    "CRON_SECRET": {
        "label": "Secret cron",
        "hint": "Bearer token des endpoints /cron/*",
        "group": "Sécurité",
        "order": 47,
        "editable": True,
    },
    "SEO_AGENT_STRICT_CONFIG": {
        "label": "Configuration stricte",
        "hint": "true en production",
        "group": "Sécurité",
        "order": 48,
        "editable": True,
    },
    "SEO_AGENT_TRUST_PROXY_HEADERS": {
        "label": "Headers proxy",
        "hint": "true sur Render/proxy de confiance",
        "group": "Sécurité",
        "order": 49,
        "editable": True,
    },
    "SEO_AGENT_CSP_ENABLED": {
        "label": "CSP active",
        "hint": "true recommandé",
        "group": "Sécurité",
        "order": 50,
        "editable": True,
    },
    "SEO_AGENT_CSP_REPORT_ONLY": {
        "label": "CSP report-only",
        "hint": "true pour observer sans bloquer",
        "group": "Sécurité",
        "order": 51,
        "editable": True,
    },
    "SEO_AGENT_CSP": {
        "label": "CSP personnalisée",
        "hint": "override avancé",
        "group": "Sécurité",
        "order": 52,
        "editable": True,
    },
    "SEO_AGENT_FILE_VIEW_MAX_BYTES": {
        "label": "Prévisualisation fichiers",
        "hint": "taille max en octets",
        "group": "Sécurité",
        "order": 53,
        "editable": True,
    },
    "SEO_AGENT_CSRF_BODY_MAX_BYTES": {
        "label": "CSRF body max",
        "hint": "taille max body formulaire",
        "group": "Sécurité",
        "order": 54,
        "editable": True,
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
    "PLAN_CONFIG_JSON": {
        "label": "Forfaits — quotas & modèles",
        "hint": "Surcharge des limites/modèles par forfait (JSON minifié, 1 ligne)",
        "group": "Facturation",
        "order": 10,
        "editable": True,
        "help": {
            "title": "Config forfaits — surcharge des quotas & moteurs IA",
            "steps": [
                "Surcharge les chiffres par défaut sans redéploiement (limites par métrique + modèle/max fichiers des corrections).",
                "Colle un JSON minifié (sur UNE ligne). Seuls les nombres et champs connus sont pris en compte ; un JSON invalide est ignoré.",
                "Métriques: ai_corrections_month, pages_crawled_month, assistant_messages_month, backlink_searches_month, backlink_replies_month, projects.",
                "Ex: {\"solo\":{\"limits\":{\"ai_corrections_month\":120},\"correction\":{\"model\":\"claude-sonnet-4-6\",\"max_files\":15}},\"business\":{\"correction\":{\"model\":\"claude-opus-4-8\",\"max_files\":40}}}",
                "Laisse vide / supprime pour revenir aux valeurs par défaut du code.",
            ],
            "links": [],
        },
    },
}

_INTERNAL_SETTINGS_KEYS: set[str] = {
    "SEO_AUDIT_ASSISTANT_PROVIDER",
    "OPENAI_API_KEY",
    "SEO_AUDIT_ASSISTANT_OPENAI_MODEL",
    "GOOGLE_GEMINI_API_KEY",
    "SEO_AUDIT_ASSISTANT_GEMINI_MODEL",
}


_SETTINGS_BOOL_KEYS = {
    "SEO_AGENT_STRICT_CONFIG",
    "SEO_AGENT_TRUST_PROXY_HEADERS",
    "SEO_AGENT_CSP_ENABLED",
    "SEO_AGENT_CSP_REPORT_ONLY",
    "SMTP_STARTTLS",
    "SMTP_SSL",
    "EMAIL_VERIFICATION_DISABLED",
}
_SETTINGS_URL_KEYS = {
    "PUBLIC_BASE_URL",
    "GOOGLE_OAUTH_REDIRECT_URI",
    "GITHUB_OAUTH_REDIRECT_URI",
    "NETLIFY_OAUTH_REDIRECT_URI",
    "BING_OAUTH_REDIRECT_URI",
}
_SETTINGS_STRONG_SECRET_KEYS = {
    "SEO_AGENT_SECRET_KEY",
    "SEO_AGENT_ENCRYPTION_KEY",
    "CRON_SECRET",
}


def _has_control_chars(value: str) -> bool:
    return any(ord(ch) < 32 or ord(ch) == 127 for ch in value)


def _validate_settings_url(value: str) -> str | None:
    try:
        parts = urlsplit(value)
    except Exception:
        return "URL invalide."
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        return "URL invalide (http/https requis)."
    if parts.username or parts.password:
        return "URL invalide (identifiants interdits)."
    return None


def _validate_settings_env_value(key: str, value: str) -> str | None:
    k = (key or "").strip()
    v = (value or "").strip()
    if not v:
        return "Valeur manquante."
    if len(v) > 8192:
        return "Valeur trop longue."
    if "\r" in v or "\n" in v or "\x00" in v:
        return "Valeur invalide (caractère de contrôle)."

    if k in _SETTINGS_BOOL_KEYS and v.lower() not in {"1", "0", "true", "false", "yes", "no", "y", "n", "on", "off"}:
        return "Valeur booléenne attendue (true/false)."

    if k in _SETTINGS_URL_KEYS:
        err = _validate_settings_url(v)
        if err:
            return err

    if k == "PLAN_CONFIG_JSON":
        try:
            parsed = json.loads(v)
        except Exception as e:
            return f"JSON invalide : {e}"
        if not isinstance(parsed, dict):
            return "Un objet JSON est attendu, ex. {\"solo\":{\"limits\":{\"ai_corrections_month\":120},\"correction\":{\"max_files\":15}}}"

    if k == "GOOGLE_APPLICATION_CREDENTIALS":
        p = Path(v).expanduser()
        if not p.is_absolute():
            p = (REPO_ROOT / p).resolve()
        else:
            p = p.resolve()
        allowed = any(p.is_relative_to(root.resolve()) for root in [REPO_ROOT, DATA_DIR])
        if not allowed:
            return "Chemin JSON refusé."
        if p.suffix.lower() != ".json":
            return "Fichier JSON attendu."

    if k in _SETTINGS_STRONG_SECRET_KEYS and _weak_secret(v):
        return "Secret trop faible (32+ caractères aléatoires requis)."

    if k == "SEO_AGENT_ENCRYPTION_KEY":
        session_secret = _safe_env("SEO_AGENT_SECRET_KEY")
        if session_secret and hmac.compare_digest(v, session_secret):
            return "La clé de chiffrement doit être différente du secret de session."

    if k == "SEO_AGENT_ENCRYPTION_KEYS":
        seeds = [part.strip() for part in re.split(r"[,\n;]+", v) if part and part.strip()]
        if not seeds:
            return "Au moins une clé de chiffrement est requise."
        if any(_weak_secret(seed) for seed in seeds):
            return "Une clé de chiffrement est trop faible."
        session_secret = _safe_env("SEO_AGENT_SECRET_KEY")
        if session_secret and any(hmac.compare_digest(seed, session_secret) for seed in seeds):
            return "Les clés de chiffrement doivent être différentes du secret de session."

    if k == "SEO_AUDIT_ASSISTANT_PROVIDER" and v.lower() not in {"auto", "gemini", "openai", "none"}:
        return "Fournisseur IA invalide."

    if k == "SMTP_PORT":
        try:
            port = int(v)
        except Exception:
            return "Port numérique attendu."
        if port < 1 or port > 65535:
            return "Port hors plage."

    if k in {
        "SMTP_TIMEOUT_SECONDS",
        "EMAIL_VERIFY_TTL_SECONDS",
        "PASSWORD_RESET_TTL_SECONDS",
        "SEO_AGENT_FILE_VIEW_MAX_BYTES",
        "SEO_AGENT_CSRF_BODY_MAX_BYTES",
    }:
        try:
            n = int(v)
        except Exception:
            return "Nombre entier attendu."
        if k in {"SEO_AGENT_FILE_VIEW_MAX_BYTES", "SEO_AGENT_CSRF_BODY_MAX_BYTES"}:
            max_bytes = 20 * 1024 * 1024 if k == "SEO_AGENT_FILE_VIEW_MAX_BYTES" else 50 * 1024 * 1024
            if n < 65536 or n > max_bytes:
                return f"Taille hors plage (65536 à {max_bytes} octets)."
        elif n < 1 or n > 60 * 60 * 24 * 30:
            return "Durée hors plage."

    if _has_control_chars(v):
        return "Valeur invalide (caractère de contrôle)."

    return None


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
                logger.error("[MAIL] forgot send_error: %s: %s", type(exc).__name__, str(exc)[:500])
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
            logger.info("[MAIL] forgot user_found=0 email=%s", _mask_email(e))
            _audit_log(request, action="auth.forgot", status="ok")

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

    now = datetime.now(timezone.utc)
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

    now = datetime.now(timezone.utc)
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
        owns_nothing = not int(
            db.scalar(select(func.count()).select_from(Project).where(Project.owner_user_id == uid)) or 0
        )

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
    # `next` is wherever the visitor happened to be when they signed up — often a page owned by
    # somebody else. A brand-new account owns nothing, so following it makes "Job introuvable"
    # the first screen of the product. Observed on a real signup: the ownership check was right,
    # the welcome was not. An account that already owns projects keeps its destination, so an
    # invite link to a real page still works.
    destination = "/" if owns_nothing else n
    resp = RedirectResponse(url=destination, status_code=303)
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
def auth_signup(
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
    invite_required = bool(_safe_env("SIGNUP_INVITE_CODE"))
    signup_disabled = _env_bool("SIGNUP_DISABLED")
    e = _normalize_email(email or "")
    resp = templates.TemplateResponse(
        "auth_signup.html",
        {
            "request": request,
            "next": n,
            "next_q": quote(n),
            "msg": str(msg or "").strip(),
            "err": str(err or "").strip(),
            "email": e,
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
    signup_window_s = 15 * 60
    retry_ip = _rate_limit_retry_after(bucket="auth_signup_ip", subject=ip, limit=20, window_s=signup_window_s)
    retry_email = _rate_limit_retry_after(
        bucket="auth_signup_email", subject=(e or "missing"), limit=10, window_s=signup_window_s
    )
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
        target = f"/auth/signup?next={quote(n)}&email={quote(e)}"
        return RedirectResponse(
            url=_path_with_flash(target, err=f"Trop de tentatives. Réessaie dans {_format_retry_after(retry_after)}."),
            status_code=303,
        )

    def _signup_error(msg: str, status_code: int = 400) -> Response:
        _audit_log(
            request,
            action="auth.signup",
            status="rejected",
            actor_email=e,
            meta={"reason": msg, "status_code": status_code},
        )
        target = f"/auth/signup?next={quote(n)}&email={quote(e)}"
        return RedirectResponse(url=_path_with_flash(target, err=msg), status_code=303)

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
            # The account was created several lines above, so bouncing back to the signup form
            # is a dead end: retrying the same address now answers "Ce compte existe déjà" and
            # never mentions that a resend page exists. Send them where they can actually
            # finish, and say the account exists so the refusal further up makes sense.
            _audit_log(
                request,
                action="auth.signup",
                status="created_unverified",
                actor_email=e,
                target_type="user",
                target_id=str(getattr(user, "id", "") or ""),
                meta={"reason": "verification_email_failed"},
            )
            return RedirectResponse(
                url=_path_with_flash(
                    f"/auth/verify/resend?next={quote(n)}&email={quote(e)}",
                    err="Ton compte est créé, mais l’email de vérification n’a pas pu partir. Renvoie-le ci-dessous.",
                ),
                status_code=303,
            )

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

    try:
        _send_welcome_email(
            to_email=str(getattr(user, "email", "") or e),
            dashboard_url=f"{_public_base_url(request)}/",
        )
    except Exception:
        pass

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


@app.api_route("/pricing", methods=["GET", "HEAD"], response_class=HTMLResponse)
def pricing_public(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "pricing_public.html",
        {
            "request": request,
            "app_name": _app_name(),
            "support_email": _support_email(),
            "year": datetime.now(timezone.utc).year,
            "nav_items": _public_nav_items(),
            "catalog": billing.plan_catalog(),
            "stripe_ok": billing.stripe_enabled(),
        },
    )


@app.api_route("/terms", methods=["GET", "HEAD"], response_class=HTMLResponse)
def terms_public(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "terms_public.html",
        {
            "request": request,
            "app_name": _app_name(),
            "support_email": _support_email(),
            "year": datetime.now(timezone.utc).year,
            "nav_items": _public_nav_items(),
            "legal_version": _legal_version(),
            "legal_updated_at": _legal_updated_at(),
        },
    )


@app.api_route("/privacy", methods=["GET", "HEAD"], response_class=HTMLResponse)
def privacy_public(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "privacy_public.html",
        {
            "request": request,
            "app_name": _app_name(),
            "support_email": _support_email(),
            "year": datetime.now(timezone.utc).year,
            "nav_items": _public_nav_items(),
            "legal_version": _legal_version(),
            "legal_updated_at": _legal_updated_at(),
        },
    )


@app.api_route("/support", methods=["GET", "HEAD"], response_class=HTMLResponse)
def support_public(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "support_public.html",
        {
            "request": request,
            "app_name": _app_name(),
            "support_email": _support_email(),
            "year": datetime.now(timezone.utc).year,
            "nav_items": _public_nav_items(),
        },
    )


@app.api_route("/status", methods=["GET", "HEAD"], response_class=HTMLResponse)
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
            "year": datetime.now(timezone.utc).year,
            "nav_items": _public_nav_items(),
            "now": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "web_ok": True,
            "db_ok": db_ok,
            "db_error": db_error,
            "smtp_ok": smtp_ok,
            "s3_ok": s3_ok,
            "s3_reason": s3_reason,
            "stripe_ok": billing.stripe_enabled(),
        },
    )


def _content_json_ld(request: Request, page: dict[str, Any], *, path: str) -> dict[str, Any]:
    """Article schema, plus FAQPage when the page carries one."""
    article = {
        "@type": "TechArticle" if page["collection"] == "docs" else "Article",
        "headline": page.get("title"),
        "description": page.get("description"),
        "dateModified": page.get("updated_at"),
        "datePublished": page.get("published_at") or page.get("updated_at"),
        "author": {"@type": "Organization", "name": _app_name()},
        "publisher": {"@type": "Organization", "name": _app_name()},
        "mainEntityOfPage": _public_url(request, path),
        "keywords": ", ".join(page.get("keywords") or []),
    }
    faq = page.get("faq") or []
    if faq:
        return {
            "@context": "https://schema.org",
            "@graph": [
                article,
                {
                    "@type": "FAQPage",
                    "mainEntity": [
                        {
                            "@type": "Question",
                            "name": item["question"],
                            "acceptedAnswer": {"@type": "Answer", "text": item["answer"]},
                        }
                        for item in faq
                    ],
                },
            ],
        }
    return {"@context": "https://schema.org", **article}


@app.api_route("/docs", methods=["GET", "HEAD"], response_class=HTMLResponse)
def docs_index_public(request: Request) -> HTMLResponse:
    tokens = _content_tokens()
    sections = [
        {"name": section["name"], "pages": content_library.resolve_all(section["pages"], tokens)}
        for section in content_library.docs_sections()
    ]
    return templates.TemplateResponse(
        "docs_index.html",
        _public_template_context(
            request,
            sections=sections,
            canonical_url=_public_url(request, "/docs"),
            meta_description=(
                f"Documentation {_app_name()} : prise en main, crawl, anomalies, corrections "
                "automatiques en pull request, Search Console, concurrents, backlinks et quotas."
            ),
        ),
    )


@app.api_route("/docs/{slug}", methods=["GET", "HEAD"], response_class=HTMLResponse)
def docs_article_public(request: Request, slug: str) -> HTMLResponse:
    raw = content_library.get_doc(slug)
    if not raw:
        raise HTTPException(status_code=404, detail="doc_not_found")
    tokens = _content_tokens()
    page = content_library.resolve(raw, tokens)
    path = f"/docs/{page['slug']}"
    return templates.TemplateResponse(
        "docs_article.html",
        _public_template_context(
            request,
            page=page,
            sections=[
                {"name": section["name"], "pages": content_library.resolve_all(section["pages"], tokens)}
                for section in content_library.docs_sections()
            ],
            related=content_library.resolve_all(content_library.related_pages(raw), tokens),
            canonical_url=_public_url(request, path),
            json_ld=_content_json_ld(request, page, path=path),
        ),
    )


@app.api_route("/ressources-seo", methods=["GET", "HEAD"], response_class=HTMLResponse)
def seo_resources_public(request: Request) -> HTMLResponse:
    tokens = _content_tokens()
    return templates.TemplateResponse(
        "seo_resources_public.html",
        _public_template_context(
            request,
            resources=content_library.resolve_all(content_library.blog_pages(), tokens),
            canonical_url=_public_url(request, "/ressources-seo"),
            meta_description=(
                "Guides SEO, tutoriels et checklists pour auditer un site, prioriser les corrections, "
                "suivre Google Search Console et améliorer le référencement naturel."
            ),
        ),
    )


@app.api_route("/ressources-seo/{slug}", methods=["GET", "HEAD"], response_class=HTMLResponse)
def seo_resource_article_public(request: Request, slug: str) -> HTMLResponse:
    raw = content_library.get_article(slug)
    if not raw:
        raise HTTPException(status_code=404, detail="resource_not_found")
    tokens = _content_tokens()
    page = content_library.resolve(raw, tokens)
    path = f"/ressources-seo/{page['slug']}"
    return templates.TemplateResponse(
        "seo_resource_article_public.html",
        _public_template_context(
            request,
            page=page,
            related=content_library.resolve_all(content_library.related_pages(raw), tokens),
            canonical_url=_public_url(request, path),
            json_ld=_content_json_ld(request, page, path=path),
        ),
    )


@app.api_route("/robots.txt", methods=["GET", "HEAD"], response_class=PlainTextResponse, include_in_schema=False)
def robots_txt(request: Request) -> PlainTextResponse:
    base = _public_base_url(request)
    sitemap_line = f"\nSitemap: {base}/sitemap.xml"
    content = f"""User-agent: *
Allow: /
Allow: /pricing
Allow: /terms
Allow: /privacy
Allow: /support
Allow: /status
Allow: /ressources-seo
Allow: /docs
Disallow: /auth/
Disallow: /billing
Disallow: /jobs
Disallow: /projects/
Disallow: /settings/
Disallow: /automation
Disallow: /api/
Disallow: /file
Disallow: /oauth/{sitemap_line}
"""
    return PlainTextResponse(content.strip(), media_type="text/plain; charset=utf-8")


@app.api_route("/sitemap.xml", methods=["GET", "HEAD"], include_in_schema=False)
def sitemap_xml(request: Request) -> PlainTextResponse:
    base = _public_base_url(request)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    urls = [
        {"path": "/", "lastmod": now, "changefreq": "weekly"},
        {"path": "/pricing", "lastmod": now, "changefreq": "monthly"},
        {"path": "/terms", "lastmod": now, "changefreq": "yearly"},
        {"path": "/privacy", "lastmod": now, "changefreq": "yearly"},
        {"path": "/support", "lastmod": now, "changefreq": "monthly"},
        {"path": "/status", "lastmod": now, "changefreq": "weekly"},
        {"path": "/ressources-seo", "lastmod": now, "changefreq": "weekly"},
        {"path": "/docs", "lastmod": now, "changefreq": "weekly"},
    ]
    urls.extend(content_library.sitemap_entries())
    url_blocks = "\n".join(
        (
            f"  <url><loc>{html.escape(base + str(item['path']))}</loc>"
            f"<lastmod>{html.escape(str(item['lastmod']))}</lastmod>"
            f"<changefreq>{html.escape(str(item['changefreq']))}</changefreq></url>"
        )
        for item in urls
    )
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
{url_blocks}
</urlset>"""
    return PlainTextResponse(xml.strip(), media_type="application/xml; charset=utf-8")


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
                try:
                    db.rollback()
                except Exception:
                    pass

        plan_key = billing.effective_plan_key(db, user_id=str(user.id))
        if stripe_ready and plan_key == "free" and billing.stripe_customer_id(db, user_id=str(user.id)):
            # A Stripe customer exists for this account but no plan is active: a payment went
            # through and was never reconciled. That state used to be unrecoverable — the
            # session-based sync needs a session_id that only exists on the redirect back from
            # payment, so closing that tab meant paying and staying on Free with no way out.
            # Everything needed is already stored; ask Stripe on every visit until it resolves.
            try:
                if billing.sync_subscription_from_customer(db, user_id=str(user.id)):
                    plan_key = billing.effective_plan_key(db, user_id=str(user.id))
                    err_out = ""
            except Exception as e:
                # A failed flush leaves the Session unusable: every later query in this request
                # raises PendingRollbackError and the page 500s. Reconciliation is best-effort,
                # so hand the request back a working session and render the plan we know about.
                logger.warning("[BILLING] customer sync failed: %s: %s", type(e).__name__, e)
                try:
                    db.rollback()
                except Exception:
                    pass
        limits = billing.plan_limits(db, user_id=str(user.id))
        sub = billing.subscription_for_user(db, user_id=str(user.id))
        sub_active = bool(sub and str(getattr(sub, "status", "") or "").strip().lower() in billing.ACTIVE_SUB_STATUSES)

        used_pages = billing.usage_sum(db, user_id=str(user.id), metric="pages_crawled_month")
        used_ai = billing.usage_sum(db, user_id=str(user.id), metric="assistant_messages_month")
        used_corrections = billing.usage_sum(db, user_id=str(user.id), metric="ai_corrections_month")
        projects_count = int(
            db.scalar(select(func.count()).select_from(Project).where(Project.owner_user_id == str(user.id))) or 0
        )
        invoices = billing.list_invoices(db, user_id=str(user.id))
        try:
            pending_change = billing.pending_plan_change(db, user_id=str(user.id)) if sub_active else None
        except Exception as e:
            # Informational only: never take the billing page down over a banner.
            logger.warning("[STRIPE] pending plan change unreadable: %s: %s", type(e).__name__, e)
            pending_change = None

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
            "pending_change": pending_change,
            "limits": limits,
            "limits_labels": {
                "projects": _limit_label("projects"),
                "pages_crawled_month": _limit_label("pages_crawled_month"),
                "assistant_messages_month": _limit_label("assistant_messages_month"),
                "ai_corrections_month": _limit_label("ai_corrections_month"),
            },
            "is_admin": bool(getattr(user, "is_admin", False)),
            "usage": {
                "projects": projects_count,
                "pages_crawled_month": used_pages,
                "assistant_messages_month": used_ai,
                "ai_corrections_month": used_corrections,
            },
            "usage_pct": {
                "pages_crawled_month": _pct(used_pages, "pages_crawled_month"),
                "assistant_messages_month": _pct(used_ai, "assistant_messages_month"),
                "ai_corrections_month": _pct(used_corrections, "ai_corrections_month"),
            },
            "catalog": catalog,
            "prices": {
                "solo": billing.price_id_for_plan("solo"),
                "pro": billing.price_id_for_plan("pro"),
                "business": billing.price_id_for_plan("business"),
            },
            "invoices": invoices,
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
                    # An upgrade is billed on the spot; a refused card leaves the plan untouched.
                    billing.change_plan_now(db, user_id=str(user.id), target_plan_key=pk)
                    _audit_log(
                        request,
                        action="billing.plan_change",
                        status="ok",
                        user=user,
                        meta={"mode": "upgrade_now", "from": current, "to": pk},
                    )
                    return RedirectResponse(url=f"/billing?msg={quote('Plan mis à jour. La différence a été facturée.')}", status_code=303)
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
            except billing.UpgradePaymentFailed as e:
                # Not an internal error: the card was refused and the plan was rolled back.
                # Say so in the customer's terms instead of surfacing a Stripe string.
                _audit_log(
                    request,
                    action="billing.plan_change",
                    status="payment_failed",
                    user=user,
                    meta={"from": current, "to": pk, "invoice_status": e.invoice_status},
                )
                return RedirectResponse(url=f"/billing?err={quote(str(e))}", status_code=303)
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


@app.post("/billing/cancel-scheduled-change")
def billing_cancel_scheduled_change(request: Request) -> RedirectResponse:
    """Undo a booked plan change: the customer keeps the plan they are on."""
    user = getattr(request.state, "user", None)
    if not user:
        return RedirectResponse(url="/auth/login", status_code=303)
    retry_after = _rate_limit_retry_after(
        bucket="billing_cancel_sched_user", subject=str(getattr(user, "id", "")), limit=10, window_s=10 * 60
    )
    if isinstance(retry_after, int):
        return RedirectResponse(
            url=_path_with_flash("/billing", err=f"Trop de tentatives. Réessaie dans {_format_retry_after(retry_after)}."),
            status_code=303,
        )
    try:
        with DB.session() as db:
            released = billing.cancel_scheduled_plan_change(db, user_id=str(user.id))
    except Exception as e:
        _audit_log(
            request, action="billing.cancel_scheduled_change", status="error", user=user,
            meta={"error": str(e)[:240]},
        )
        return RedirectResponse(
            url=_path_with_flash("/billing", err="Impossible d'annuler le changement pour le moment. Réessaie dans un instant."),
            status_code=303,
        )
    _audit_log(
        request, action="billing.cancel_scheduled_change",
        status="ok" if released else "nothing_to_cancel", user=user,
    )
    if not released:
        # Already gone (a second click, or the change landed meanwhile): say so plainly rather
        # than claim to have undone something.
        return RedirectResponse(url=_path_with_flash("/billing", msg="Aucun changement de plan n'était programmé."), status_code=303)
    return RedirectResponse(
        url=_path_with_flash("/billing", msg="Changement de plan annulé. Tu restes sur ton plan actuel."),
        status_code=303,
    )


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
            logger.error("[STRIPE] webhook error: %s: %s", type(e).__name__, e)
            return JSONResponse({"ok": False, "error": "webhook_handler_error"}, status_code=500)

    return JSONResponse({"ok": True})


@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
def projects(request: Request, msg: str | None = None, err: str | None = None) -> HTMLResponse:
    config_path = DEFAULT_CONFIG if DEFAULT_CONFIG.exists() else None
    runs_dir = _runs_dir_for_request(request)

    user = getattr(request.state, "user", None)
    if not user:
        return templates.TemplateResponse(
            "home_public.html",
            _public_template_context(
                request,
                catalog=billing.plan_catalog(),
                featured_resources=content_library.resolve_all(
                    content_library.featured_articles(3), _content_tokens()
                ),
                canonical_url=_public_url(request, "/"),
            ),
        )
    try:
        with DB.session() as db:
            db_projects = list(
                db.scalars(select(Project).where(Project.owner_user_id == str(user.id)).order_by(Project.site_name))
                if user
                else []
            )
    except Exception as _e:
        logger.error("[projects] projects query failed: %s: %s", type(_e).__name__, _e)
        db_projects = []

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

    try:
        jobs = _list_jobs(limit=100)
    except Exception as _e:
        logger.error("[projects] _list_jobs failed: %s: %s", type(_e).__name__, _e)
        jobs = []
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

    onboarding = _dashboard_onboarding_state(
        user=user,
        projects=projects,
        recent_crawl_jobs=recent_crawl_jobs,
        live_crawls=live_crawls,
    )

    resp = templates.TemplateResponse(
        "projects.html",
        {
            "request": request,
            "config_path": str(config_path) if config_path else None,
            "projects": projects,
            "jobs": jobs,
            "live_crawls": live_crawls,
            "recent_crawl_jobs": recent_crawl_jobs,
            "onboarding": onboarding,
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
def settings_accounts(
    request: Request,
    msg: str | None = None,
    err: str | None = None,
) -> HTMLResponse:
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
        gsc_status = _gsc_live_credentials_status(user_id=str(user.id), slug=slug)
        gsc_projects.append(
            {
                "slug": slug,
                "site_name": str(proj.site_name or slug),
                "base_url": str(proj.base_url or ""),
                "gsc_enabled": bool(effective_gsc.get("enabled")) if "enabled" in effective_gsc else True,
                "oauth_connected": _gsc_oauth_connected(str(user.id), slug),
                "oauth_status": gsc_status,
                "oauth_status_hint": _gsc_oauth_status_hint(str(gsc_status.get("reason") or "")),
                "connect_url": f"/projects/{slug}/gsc/oauth/connect?{urlencode({'next': gsc_return_to})}",
                "disconnect_url": f"/projects/{slug}/gsc/oauth/disconnect",
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
            "msg": str(msg or "").strip(),
            "err": str(err or "").strip(),
            "user_prefs": {
                "timezone": str(getattr(user, "timezone", "") or ""),
                "country": str(getattr(user, "country", "") or ""),
                "language": str(getattr(user, "language", "") or ""),
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
        return RedirectResponse(
            url=_path_with_flash("/settings/accounts", err=f"{type(e).__name__}: {str(e)[:180]}"),
            status_code=303,
        )

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
    project_slugs: list[str] = []
    gsc_refresh_tokens: list[str] = []
    stripe_summary: dict[str, Any] = {}

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
            if uid:
                projects = list(db.scalars(select(Project).where(Project.owner_user_id == uid)))
                project_slugs = [str(getattr(project, "slug", "") or "").strip() for project in projects if str(getattr(project, "slug", "") or "").strip()]
                gsc_rows = list(
                    db.scalars(
                        select(UserConnection).where(
                            UserConnection.user_id == uid,
                            UserConnection.key.like("GSC_OAUTH:%"),
                        )
                    )
                )
                seen_tokens: set[str] = set()
                for row in gsc_rows:
                    stored = str(getattr(row, "secret_value", "") or "").strip()
                    if not stored:
                        continue
                    try:
                        refresh_token, _rotated = _decrypt_secret_with_rotation(stored)
                    except Exception:
                        refresh_token = ""
                    token = str(refresh_token or "").strip()
                    if token and token not in seen_tokens:
                        seen_tokens.add(token)
                        gsc_refresh_tokens.append(token)
                stripe_summary = billing.cancel_and_purge_customer(db, user_id=uid)
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
        reason = "Erreur suppression compte (Stripe)." if "stripe_" in str(e).lower() else "Erreur suppression compte (DB)."
        _audit_log(
            request,
            action="account.delete",
            status="delete_error",
            actor_email=email,
            target_type="user",
            target_id=uid,
            meta={"error": f"{type(e).__name__}: {str(e)[:200]}"},
        )
        return RedirectResponse(
            url=_path_with_flash("/settings/accounts#danger-zone", err=reason),
            status_code=303,
        )

    try:
        if uid:
            user_runs_dir = (DEFAULT_RUNS_DIR / uid).resolve()
            if user_runs_dir.exists() and user_runs_dir.is_dir():
                _delete_runs_path_from_object_store(user_runs_dir, recursive=True)
                shutil.rmtree(str(user_runs_dir), ignore_errors=True)
            for slug in project_slugs:
                legacy_runs_dir = (DEFAULT_RUNS_DIR / _safe_storage_segment(slug, "project")).resolve()
                if legacy_runs_dir.exists() and legacy_runs_dir.is_dir():
                    _delete_runs_path_from_object_store(legacy_runs_dir, recursive=True)
                    shutil.rmtree(str(legacy_runs_dir), ignore_errors=True)
    except Exception:
        pass

    try:
        if uid:
            user_gsc_dir = _gsc_oauth_user_dir(uid, create=False)
            runtime_gsc_dir = _gsc_runtime_oauth_dir(uid, create=False)
            for path in [user_gsc_dir, runtime_gsc_dir]:
                if path.exists() and path.is_dir():
                    shutil.rmtree(str(path), ignore_errors=True)
            for slug in project_slugs:
                legacy_token = (GSC_OAUTH_DIR / f"{_safe_storage_segment(slug, 'project')}.json").resolve()
                if legacy_token.exists() and legacy_token.is_file():
                    legacy_token.unlink()
    except Exception:
        pass

    revoked_tokens = 0
    for token in gsc_refresh_tokens:
        try:
            _google_oauth_revoke_token(token)
            revoked_tokens += 1
        except Exception:
            continue

    resp = RedirectResponse(url=_path_with_flash("/auth/login", msg="Compte supprimé."), status_code=303)
    resp.delete_cookie(auth.SESSION_COOKIE_NAME, path="/")
    _audit_log(
        request,
        action="account.delete",
        status="ok",
        actor_email=email,
        target_type="user",
        target_id=uid,
        meta={
            "project_count": len(project_slugs),
            "gsc_tokens_revoked": revoked_tokens,
            "stripe": stripe_summary,
        },
    )
    return resp


@app.post("/settings/account/password")
def settings_account_password(
    request: Request,
    current_password: str = Form(default=""),
    new_password: str = Form(default=""),
    new_password2: str = Form(default=""),
) -> RedirectResponse:
    user = getattr(request.state, "user", None)
    if not user:
        raise HTTPException(status_code=401, detail="auth_required")

    back = "/settings/accounts#security-card"

    cur = (current_password or "").strip()
    pw1 = (new_password or "").strip()
    pw2 = (new_password2 or "").strip()

    if not cur or not pw1 or not pw2:
        return RedirectResponse(url=_path_with_flash(back, err="Tous les champs sont requis."), status_code=303)

    if pw1 != pw2:
        return RedirectResponse(url=_path_with_flash(back, err="Les nouveaux mots de passe ne correspondent pas."), status_code=303)

    if len(pw1) < 10:
        return RedirectResponse(url=_path_with_flash(back, err="Le nouveau mot de passe doit faire au moins 10 caractères."), status_code=303)

    with DB.session() as db:
        fresh = db.get(User, str(user.id))
        if not fresh:
            raise HTTPException(status_code=401, detail="auth_required")
        if not auth.verify_password(cur, str(fresh.password_hash or "")):
            _audit_log(request, action="account.password_change", status="wrong_current_password", user=user)
            return RedirectResponse(url=_path_with_flash(back, err="Mot de passe actuel incorrect."), status_code=303)
        fresh.password_hash = auth.hash_password(pw1)
        db.add(fresh)
        db.commit()

    _audit_log(request, action="account.password_change", status="ok", user=user)
    return RedirectResponse(url=_path_with_flash(back, msg="Mot de passe mis à jour."), status_code=303)


_VALID_TIMEZONES: set[str] = {
    "Africa/Abidjan","Africa/Accra","Africa/Cairo","Africa/Casablanca","Africa/Lagos",
    "Africa/Nairobi","Africa/Tunis","Africa/Johannesburg",
    "America/Anchorage","America/Argentina/Buenos_Aires","America/Bogota","America/Chicago",
    "America/Denver","America/Halifax","America/Havana","America/Honolulu","America/Lima",
    "America/Los_Angeles","America/Mexico_City","America/New_York","America/Phoenix",
    "America/Santiago","America/Sao_Paulo","America/Toronto","America/Vancouver",
    "Asia/Almaty","Asia/Baghdad","Asia/Bangkok","Asia/Colombo","Asia/Dubai",
    "Asia/Hong_Kong","Asia/Jakarta","Asia/Karachi","Asia/Kathmandu","Asia/Kolkata",
    "Asia/Kuala_Lumpur","Asia/Manila","Asia/Seoul","Asia/Shanghai","Asia/Singapore",
    "Asia/Taipei","Asia/Tashkent","Asia/Tehran","Asia/Tokyo","Asia/Yekaterinburg",
    "Atlantic/Azores","Atlantic/Reykjavik",
    "Australia/Adelaide","Australia/Brisbane","Australia/Melbourne","Australia/Perth","Australia/Sydney",
    "Europe/Amsterdam","Europe/Athens","Europe/Belgrade","Europe/Berlin","Europe/Brussels",
    "Europe/Bucharest","Europe/Budapest","Europe/Copenhagen","Europe/Dublin","Europe/Helsinki",
    "Europe/Istanbul","Europe/Kiev","Europe/Lisbon","Europe/Ljubljana","Europe/London",
    "Europe/Madrid","Europe/Minsk","Europe/Moscow","Europe/Oslo","Europe/Paris",
    "Europe/Prague","Europe/Riga","Europe/Rome","Europe/Sofia","Europe/Stockholm",
    "Europe/Tallinn","Europe/Vienna","Europe/Vilnius","Europe/Warsaw","Europe/Zurich",
    "Pacific/Auckland","Pacific/Fiji","Pacific/Honolulu","Pacific/Noumea","Pacific/Tahiti",
    "UTC",
}
_VALID_LANGUAGES: set[str] = {"ar","de","en","es","fr","it","ja","ko","nl","pl","pt","ru","tr","zh"}
_VALID_COUNTRIES: set[str] = {
    "AE","AR","AT","AU","BE","BR","CA","CH","CL","CN","CO","CZ","DE","DK","EG","ES",
    "FI","FR","GB","GR","HK","HR","HU","ID","IE","IL","IN","IT","JP","KR","MA","MX",
    "MY","NG","NL","NO","NZ","PE","PH","PL","PT","RO","RU","SA","SE","SG","TH","TR",
    "TW","UA","US","VN","ZA",
}


@app.post("/settings/account/preferences")
def settings_account_preferences_save(
    request: Request,
    timezone: str = Form(default=""),
    country: str = Form(default=""),
    language: str = Form(default=""),
) -> RedirectResponse:
    user = getattr(request.state, "user", None)
    if not user:
        return RedirectResponse(url="/auth/login", status_code=303)  # type: ignore[return-value]
    tz = timezone.strip() if timezone.strip() in _VALID_TIMEZONES else None
    co = country.strip().upper() if country.strip().upper() in _VALID_COUNTRIES else None
    la = language.strip().lower() if language.strip().lower() in _VALID_LANGUAGES else None
    with DB.session() as db:
        u = db.get(User, str(user.id))
        if u:
            u.timezone = tz
            u.country = co
            u.language = la
            db.commit()
    return RedirectResponse(
        url=_path_with_flash("/settings/accounts", msg="Préférences enregistrées.") + "#preferences-card",
        status_code=303,
    )


_OPS_OK_AUDIT_STATUSES = {"ok", "saved", "cleared", "noop", "requested"}


def _ops_check(*, group: str, label: str, ok: bool, detail: str = "", severity: str = "error") -> dict[str, Any]:
    status = "ok" if ok else ("warning" if severity == "warning" else "error")
    return {
        "group": group,
        "label": label,
        "status": status,
        "detail": str(detail or "").strip(),
    }


def _ops_badge(status: str) -> str:
    value = str(status or "").strip().lower()
    if value in {"ok", "done", "active", "trialing"}:
        return "ok"
    if value in {"warning", "queued", "running", "notice", "noop", "saved", "cleared"}:
        return "warning"
    if value in {"error", "failed", "canceled"}:
        return "error"
    return "notice"


def _ops_unix_ts_label(value: float | int | None) -> str:
    if not value:
        return ""
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc).strftime("%d/%m/%y %H:%M UTC")
    except Exception:
        return ""


def _ops_dt_label(value: datetime | None) -> str:
    if not value:
        return ""
    try:
        dt_value = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt_value.astimezone(timezone.utc).strftime("%d/%m/%y %H:%M UTC")
    except Exception:
        return ""


def _ops_age_label(seconds: float | int | None) -> str:
    if seconds is None:
        return ""
    try:
        return _fmt_duration(max(0, int(seconds)))
    except Exception:
        return ""


def _ops_bytes_label(value: int | float | None) -> str:
    try:
        amount = float(value or 0)
    except Exception:
        amount = 0.0
    units = ["o", "Ko", "Mo", "Go", "To"]
    idx = 0
    while amount >= 1024 and idx < len(units) - 1:
        amount /= 1024
        idx += 1
    if idx == 0:
        return f"{int(amount)} {units[idx]}"
    return f"{amount:.1f} {units[idx]}"


def _ops_disk_snapshot(label: str, path: Path) -> dict[str, Any]:
    target = path if path.exists() else path.parent
    try:
        usage = shutil.disk_usage(target)
        free_pct = (usage.free / usage.total * 100.0) if usage.total else 0.0
        status = "ok" if free_pct >= 20 else ("warning" if free_pct >= 10 else "error")
        return {
            "label": label,
            "path": str(path),
            "status": status,
            "total": _ops_bytes_label(usage.total),
            "used": _ops_bytes_label(usage.used),
            "free": _ops_bytes_label(usage.free),
            "free_pct": round(free_pct, 1),
        }
    except Exception as e:
        return {
            "label": label,
            "path": str(path),
            "status": "warning",
            "total": "-",
            "used": "-",
            "free": "-",
            "free_pct": 0,
            "error": f"{type(e).__name__}: {str(e)[:160]}",
        }


def _ops_job_item(job: JobRecord, *, now_ts: float) -> dict[str, Any]:
    result = job.result if isinstance(job.result, dict) else {}
    updated_at = float(job.updated_at or job.created_at or 0)
    return {
        "id": str(job.id or ""),
        "short_id": str(job.id or "")[:8],
        "status": str(job.status or ""),
        "status_badge": _ops_badge(str(job.status or "")),
        "kind": str(job.kind or _job_kind_from_command(job.command) or result.get("type") or ""),
        "slug": str(job.slug or result.get("slug") or ""),
        "owner_user_id": str(job.owner_user_id or result.get("user_id") or ""),
        "created_at": _ops_unix_ts_label(job.created_at),
        "updated_at": _ops_unix_ts_label(updated_at),
        "age": _ops_age_label(now_ts - updated_at) if updated_at else "",
        "attempts": int(job.attempts or 0),
        "error": ((str(job.stderr or "").strip().splitlines() or [""])[-1])[:240],
    }


def _ops_audit_item(row: AuditLog) -> dict[str, Any]:
    return {
        "created_at": _ops_dt_label(row.created_at),
        "action": str(row.action or ""),
        "status": str(row.status or ""),
        "status_badge": _ops_badge(str(row.status or "")),
        "actor": _mask_email(str(row.actor_email or "")) if row.actor_email else "",
        "target_type": str(row.target_type or ""),
        "target_id": str(row.target_id or "")[:96],
    }


def _production_operations_snapshot() -> dict[str, Any]:
    now_ts = time.time()
    now_dt = datetime.now(timezone.utc)
    checks: list[dict[str, Any]] = []

    public_base = _safe_env("PUBLIC_BASE_URL").rstrip("/")
    public_error = _validate_settings_url(public_base) if public_base else "PUBLIC_BASE_URL manquant."
    public_scheme = (urlsplit(public_base).scheme or "").lower() if public_base else ""
    checks.append(
        _ops_check(
            group="Configuration",
            label="PUBLIC_BASE_URL HTTPS",
            ok=bool(public_base and not public_error and public_scheme == "https"),
            detail=public_error or ("OK" if public_scheme == "https" else "HTTPS requis en production."),
        )
    )
    checks.append(
        _ops_check(
            group="Configuration",
            label="Mode strict production",
            ok=_strict_config_enabled(),
            detail="SEO_AGENT_STRICT_CONFIG actif." if _strict_config_enabled() else "Active SEO_AGENT_STRICT_CONFIG sur Render.",
            severity="warning",
        )
    )
    try:
        _validate_startup_config()
        strict_detail = "Validation startup OK." if _strict_config_enabled() else "Non bloquant tant que le mode strict est désactivé."
        checks.append(
            _ops_check(
                group="Configuration",
                label="Validation startup",
                ok=True,
                detail=strict_detail,
            )
        )
    except Exception as e:
        checks.append(
            _ops_check(
                group="Configuration",
                label="Validation startup",
                ok=False,
                detail=str(e)[:300],
            )
        )

    checks.extend(
        [
            _ops_check(
                group="Secrets",
                label="DATABASE_URL",
                ok=bool(_safe_env("DATABASE_URL")),
                detail="Postgres configuré." if _safe_env("DATABASE_URL") else "DATABASE_URL manquant.",
            ),
            _ops_check(
                group="Secrets",
                label="SEO_AGENT_SECRET_KEY",
                ok=not _weak_secret(_safe_env("SEO_AGENT_SECRET_KEY")),
                detail="Secret session robuste." if not _weak_secret(_safe_env("SEO_AGENT_SECRET_KEY")) else "Secret session trop faible ou absent.",
            ),
            _ops_check(
                group="Secrets",
                label="Chiffrement secrets DB",
                ok=bool(_secret_storage_health().get("configured")),
                detail="Chiffrement actif." if bool(_secret_storage_health().get("configured")) else "Configure SEO_AGENT_ENCRYPTION_KEY(S).",
            ),
            _ops_check(
                group="Secrets",
                label="CRON_SECRET",
                ok=not _weak_secret(_safe_env("CRON_SECRET")),
                detail="Secret cron robuste." if not _weak_secret(_safe_env("CRON_SECRET")) else "CRON_SECRET faible ou absent.",
            ),
        ]
    )

    smtp_ready = bool(_smtp_config())
    stripe_ready = bool(
        billing.stripe_enabled()
        and _safe_env("STRIPE_WEBHOOK_SECRET")
        and billing.price_id_for_plan("solo")
        and billing.price_id_for_plan("pro")
        and billing.price_id_for_plan("business")
    )
    assistant_ready = _assistant_effective_provider() != "none"
    checks.extend(
        [
            _ops_check(
                group="Produit",
                label="Emails transactionnels",
                ok=smtp_ready,
                detail="SMTP/SendGrid configuré." if smtp_ready else "SMTP non configuré: reset password et vérification email indisponibles.",
                severity="warning",
            ),
            _ops_check(
                group="Produit",
                label="Stripe live",
                ok=stripe_ready,
                detail="Stripe + prix + webhook configurés." if stripe_ready else "Vérifie STRIPE_SECRET_KEY, STRIPE_WEBHOOK_SECRET et les price ids.",
                severity="warning",
            ),
            _ops_check(
                group="Produit",
                label="Assistant IA",
                ok=assistant_ready,
                detail=f"Provider actif: {_assistant_effective_provider()}." if assistant_ready else "Aucun provider IA configuré.",
                severity="warning",
            ),
        ]
    )

    checks.extend(
        [
            _ops_check(
                group="Sécurité",
                label="CSP active",
                ok=_csp_enabled(),
                detail="CSP active." if _csp_enabled() else "Réactive SEO_AGENT_CSP_ENABLED avant ouverture.",
                severity="warning",
            ),
            _ops_check(
                group="Sécurité",
                label="Headers proxy",
                ok=_trust_proxy_headers(),
                detail="Headers proxy pris en compte." if _trust_proxy_headers() else "Active SEO_AGENT_TRUST_PROXY_HEADERS sur Render.",
                severity="warning",
            ),
        ]
    )

    backup_ready = bool(_safe_env("S3_BUCKET_NAME") and _safe_env("AWS_ACCESS_KEY_ID") and _safe_env("AWS_SECRET_ACCESS_KEY"))
    object_store_ready = object_store.s3_enabled()
    checks.extend(
        [
            _ops_check(
                group="Données",
                label="Object storage runs",
                ok=object_store_ready,
                detail="S3 runs actif." if object_store_ready else f"S3 runs inactif ({object_store.s3_available_reason() or 'non configuré'}).",
                severity="warning",
            ),
            _ops_check(
                group="Données",
                label="Backups S3",
                ok=backup_ready,
                detail="Backup S3 configurable." if backup_ready else "Configure S3_BUCKET_NAME et credentials AWS pour le cron backup.",
                severity="warning",
            ),
        ]
    )

    db_stats: dict[str, Any] = {
        "ok": False,
        "users": 0,
        "projects": 0,
        "jobs": {},
        "active_subscriptions": 0,
        "recent_audit_problem_count": 0,
    }
    recent_problem_jobs: list[dict[str, Any]] = []
    stale_jobs: list[dict[str, Any]] = []
    recent_audit: list[dict[str, Any]] = []
    try:
        with DB.session() as db:
            db_stats["users"] = int(db.scalar(select(func.count()).select_from(User)) or 0)
            db_stats["projects"] = int(db.scalar(select(func.count()).select_from(Project)) or 0)
            db_stats["active_subscriptions"] = int(
                db.scalar(
                    select(func.count())
                    .select_from(BillingSubscription)
                    .where(BillingSubscription.status.in_(billing.ACTIVE_SUB_STATUSES))
                )
                or 0
            )
            db_stats["jobs"] = {
                str(status or "unknown"): int(count or 0)
                for status, count in db.execute(select(JobRecord.status, func.count()).group_by(JobRecord.status)).all()
            }
            recent_problem_rows = list(
                db.scalars(
                    select(JobRecord)
                    .where(JobRecord.status.in_(["failed", "canceled"]))
                    .order_by(JobRecord.updated_at.desc())
                    .limit(10)
                )
            )
            recent_problem_jobs = [_ops_job_item(row, now_ts=now_ts) for row in recent_problem_rows]
            stale_cutoff = now_ts - (2 * 60 * 60)
            stale_rows = list(
                db.scalars(
                    select(JobRecord)
                    .where(JobRecord.status.in_(["queued", "running", "cancel_requested"]))
                    .where(JobRecord.updated_at < stale_cutoff)
                    .order_by(JobRecord.updated_at.asc())
                    .limit(20)
                )
            )
            stale_jobs = [_ops_job_item(row, now_ts=now_ts) for row in stale_rows]
            recent_audit_rows = list(db.scalars(select(AuditLog).order_by(AuditLog.created_at.desc()).limit(20)))
            recent_audit = [_ops_audit_item(row) for row in recent_audit_rows]
            audit_cutoff = now_dt - timedelta(hours=24)
            db_stats["recent_audit_problem_count"] = int(
                db.scalar(
                    select(func.count())
                    .select_from(AuditLog)
                    .where(AuditLog.created_at >= audit_cutoff)
                    .where(AuditLog.status.notin_(_OPS_OK_AUDIT_STATUSES))
                )
                or 0
            )
            db_stats["ok"] = True
    except Exception as e:
        db_stats["error"] = f"{type(e).__name__}: {str(e)[:240]}"

    checks.append(
        _ops_check(
            group="Données",
            label="Base de données",
            ok=bool(db_stats.get("ok")),
            detail="Lecture DB OK." if db_stats.get("ok") else str(db_stats.get("error") or "Lecture DB impossible."),
        )
    )
    checks.append(
        _ops_check(
            group="Exécution",
            label="Jobs bloqués",
            ok=not stale_jobs,
            detail="Aucun job actif > 2h." if not stale_jobs else f"{len(stale_jobs)} job(s) actif(s) depuis plus de 2h.",
            severity="warning",
        )
    )

    status_counts = {
        "ok": sum(1 for item in checks if item["status"] == "ok"),
        "warning": sum(1 for item in checks if item["status"] == "warning"),
        "error": sum(1 for item in checks if item["status"] == "error"),
    }
    return {
        "generated_at": now_dt.strftime("%d/%m/%y %H:%M UTC"),
        "ready": status_counts["error"] == 0,
        "status_counts": status_counts,
        "checks": checks,
        "db": db_stats,
        "recent_problem_jobs": recent_problem_jobs,
        "stale_jobs": stale_jobs,
        "recent_audit": recent_audit,
        "disk": [
            _ops_disk_snapshot("Data", DATA_DIR),
            _ops_disk_snapshot("Runs", DEFAULT_RUNS_DIR),
        ],
        "integrations": {
            "google_oauth": bool(_google_oauth_client()[0] and _google_oauth_client()[1]),
            "github_oauth": bool(_github_oauth_client()[0] and _github_oauth_client()[1]),
            "netlify_oauth": bool(_netlify_oauth_client_id()),
            "bing_oauth": bool(_bing_oauth_client()[0] and _bing_oauth_client()[1]),
        },
    }


def _dashboard_onboarding_state(
    *,
    user: User,
    projects: list[dict[str, Any]],
    recent_crawl_jobs: dict[str, dict[str, Any]],
    live_crawls: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    user_id = str(getattr(user, "id", "") or "")
    has_projects = bool(projects)
    crawled_projects = [p for p in projects if str(p.get("timestamp") or "").strip()]
    has_completed_crawl = bool(crawled_projects)
    has_active_crawl = bool(live_crawls)
    has_attempted_crawl = has_completed_crawl or has_active_crawl or any(
        str(job.get("status") or "") in {"queued", "running", "done", "failed", "canceled", "cancel_requested"}
        for job in recent_crawl_jobs.values()
    )
    first_project = projects[0] if projects else {}
    first_slug = str(first_project.get("slug") or "").strip()
    first_crawled_slug = str((crawled_projects[0].get("slug") if crawled_projects else "") or "").strip()

    gsc_connected = False
    if user_id:
        for project in projects:
            slug = str(project.get("slug") or "").strip()
            if slug and _gsc_oauth_connected(user_id, slug):
                gsc_connected = True
                break

    github_connected = False
    try:
        with DB.session() as db:
            github_connected = bool(_build_github_connection_state(user_id=user_id, db=db).get("connected"))
    except Exception:
        github_connected = False

    can_review_issues = bool(first_crawled_slug)
    steps = [
        {
            "key": "project",
            "label": "Ajouter un site",
            "doc_href": "/docs/ajouter-un-projet",
            "done": has_projects,
            "detail": "Projet créé." if has_projects else "Crée le premier projet à auditer.",
            "action_label": "Ajouter",
            "action_href": "",
            "action_dialog": "add-project-dialog",
        },
        {
            "key": "crawl",
            "label": "Lancer un premier crawl",
            "doc_href": "/docs/lancer-un-crawl",
            "done": has_attempted_crawl,
            "detail": (
                "Crawl terminé."
                if has_completed_crawl
                else ("Crawl en cours." if has_active_crawl else "Analyse les pages du site pour générer le rapport.")
            ),
            "action_label": "Démarrer",
            "action_href": f"/projects/{first_slug}" if first_slug else "",
            "disabled": not first_slug,
        },
        {
            "key": "issues",
            "label": "Traiter les anomalies",
            "doc_href": "/docs/anomalies-et-priorites",
            "done": can_review_issues,
            "detail": "Rapport disponible." if can_review_issues else "Les anomalies apparaîtront après le premier crawl.",
            "action_label": "Voir",
            "action_href": f"/projects/{first_crawled_slug}/issues" if first_crawled_slug else "",
            "disabled": not first_crawled_slug,
        },
        {
            "key": "gsc",
            "label": "Connecter Search Console",
            "doc_href": "/docs/connecter-search-console",
            "done": gsc_connected,
            "detail": "Données search connectées." if gsc_connected else "Ajoute les performances réelles de recherche.",
            "action_label": "Connecter",
            "action_href": "/settings/accounts#gsc-oauth-card",
            "optional": True,
        },
        {
            "key": "github",
            "label": "Connecter GitHub",
            "doc_href": "/docs/connecter-github",
            "done": github_connected,
            "detail": "Correction via PR disponible." if github_connected else "Permet de proposer des corrections dans le code.",
            "action_label": "Connecter",
            "action_href": "/settings/accounts#github-connect-card",
            "optional": True,
        },
    ]
    required = [step for step in steps if not step.get("optional")]
    required_done = sum(1 for step in required if step["done"])
    total_required = len(required)
    done_total = sum(1 for step in steps if step["done"])
    return {
        "steps": steps,
        "required_done": required_done,
        "total_required": total_required,
        "done_total": done_total,
        "total": len(steps),
        "progress_pct": int(round((required_done / total_required) * 100)) if total_required else 100,
        "complete": required_done >= total_required,
    }


@app.get("/settings/operations", response_class=HTMLResponse)
def settings_operations(request: Request) -> HTMLResponse:
    _ = _require_system_owner(request)
    snapshot = _production_operations_snapshot()
    resp = templates.TemplateResponse(
        "settings_operations.html",
        {
            "request": request,
            "project": None,
            "snapshot": snapshot,
        },
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.get("/api/settings/operations", response_class=JSONResponse)
def api_settings_operations(request: Request) -> JSONResponse:
    _ = _require_system_owner(request)
    return JSONResponse({"ok": True, "snapshot": _production_operations_snapshot()})


@app.get("/settings/system", response_class=HTMLResponse)
def settings_system(request: Request) -> HTMLResponse:
    _ = _require_system_owner(request)
    msg = str(request.query_params.get("msg") or "").strip()
    err = str(request.query_params.get("err") or "").strip()

    system_sections = [
        {
            "id": "security-system",
            "title": "Sécurité — secrets serveur",
            "description": "Secrets de signature, chiffrement et crons. En production, ils doivent être longs, aléatoires et distincts.",
            "items": [
                _build_env_setting_item("PUBLIC_BASE_URL"),
                _build_env_setting_item("SEO_AGENT_STRICT_CONFIG"),
                _build_env_setting_item("SEO_AGENT_SECRET_KEY"),
                _build_env_setting_item("SEO_AGENT_ENCRYPTION_KEY"),
                _build_env_setting_item("SEO_AGENT_ENCRYPTION_KEYS"),
                _build_env_setting_item("CRON_SECRET"),
                _build_env_setting_item("SEO_AGENT_TRUST_PROXY_HEADERS"),
                _build_env_setting_item("SEO_AGENT_CSP_ENABLED"),
                _build_env_setting_item("SEO_AGENT_CSP_REPORT_ONLY"),
                _build_env_setting_item("SEO_AGENT_CSP"),
                _build_env_setting_item("SEO_AGENT_FILE_VIEW_MAX_BYTES"),
                _build_env_setting_item("SEO_AGENT_CSRF_BODY_MAX_BYTES"),
            ],
        },
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
            "secret_storage": _secret_storage_health(),
            "gsc": {
                "credentials": cred_info,
                "candidates": candidates,
                "help": _SETTINGS_ENV_KEYS.get("GOOGLE_APPLICATION_CREDENTIALS", {}).get("help"),
            },
        },
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.post("/settings/system/secrets/rotate")
def settings_system_rotate_secrets(request: Request) -> RedirectResponse:
    _ = _require_system_owner(request)
    try:
        counts = _rotate_user_connection_secrets()
    except Exception as e:
        _audit_log(
            request,
            action="settings.system.secret_rotation",
            status="error",
            user=request.state.user,
            target_type="user_connections",
            target_id="rotate",
            meta={"error": str(e)[:240]},
        )
        return RedirectResponse(
            url=_path_with_flash("/settings/system#secret-storage", err=f"{type(e).__name__}: {str(e)[:180]}"),
            status_code=303,
        )
    _audit_log(
        request,
        action="settings.system.secret_rotation",
        status="ok",
        user=request.state.user,
        target_type="user_connections",
        target_id="rotate",
        meta=counts,
    )
    return RedirectResponse(
        url=_path_with_flash(
            "/settings/system#secret-storage",
            msg=(
                f"Rotation terminée ({counts['rotated']} mis à jour, "
                f"{counts['unchanged']} déjà OK, {counts['unreadable']} illisibles)."
            ),
        ),
        status_code=303,
    )


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
            validation_err = _validate_settings_env_value(key, v)
            if validation_err:
                return RedirectResponse(url=_path_with_flash("/settings/system", err=validation_err), status_code=303)
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
def assistant_meta(request: Request) -> JSONResponse:
    if not getattr(request.state, "user", None):
        return JSONResponse({"ok": False, "error": "auth_required"}, status_code=401)
    effective = _assistant_effective_provider()
    configured = effective != "none"
    # Return only what the frontend needs — do not expose provider names to users
    return JSONResponse({"ok": True, "configured": configured})


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
        elif provider == "claude":
            # Claude Messages API: system is a top-level param, history uses user/assistant roles
            claude_messages = [{"role": h["role"], "content": h["content"]} for h in history]
            claude_messages.append({"role": "user", "content": message})
            reply = _assistant_claude_chat(claude_messages, system=system, model=model)
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
        logger.error("[BILLING] assistant usage error: %s: %s", type(e).__name__, e)

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
def project_gsc_oauth_disconnect(
    request: Request,
    slug: str,
    next: str = Form(default="/settings/accounts#gsc-oauth-card"),
) -> RedirectResponse:
    _ = _db_project_or_404(request, slug)
    user = getattr(request.state, "user", None)
    token = _gsc_oauth_refresh_token(str(getattr(user, "id", "")), slug)
    return_to = _safe_next_path(next or f"/projects/{slug}/settings/crawl#gsc")
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
    return RedirectResponse(url=_path_with_flash(return_to, msg="Google déconnecté."), status_code=303)


@app.post("/settings/accounts/gsc/disconnect-all")
def gsc_disconnect_all_projects(request: Request) -> RedirectResponse:
    user = getattr(request.state, "user", None)
    if not user:
        return RedirectResponse(url="/auth/login", status_code=303)
    with DB.session() as db:
        projects = list(db.scalars(select(Project).where(Project.owner_user_id == str(user.id))))
    count = 0
    for proj in projects:
        slug = str(getattr(proj, "slug", "") or "").strip()
        if not slug:
            continue
        if _gsc_oauth_connected(str(user.id), slug):
            token = _gsc_oauth_refresh_token(str(user.id), slug)
            if token:
                _google_oauth_revoke_token(token)
            _gsc_oauth_clear(str(user.id), slug)
            count += 1
    msg = f"{count} connexion{'s' if count > 1 else ''} Google déconnectée{'s' if count > 1 else ''}." if count else "Aucune connexion Google active."
    _audit_log(request, action="oauth.google.disconnect-all", status="ok", user=user)
    return RedirectResponse(url=_path_with_flash("/settings/accounts#gsc-oauth-card", msg=msg), status_code=303)


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
            "connected_at": datetime.now(timezone.utc).isoformat(timespec="seconds") + "Z",
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
            "connected_at": datetime.now(timezone.utc).isoformat(timespec="seconds") + "Z",
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
    proj = _db_project_or_404(request, slug)
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
        reason = _classify_google_oauth_failure(e)
        if reason == "oauth_invalid_grant":
            _clear_stale_gsc_oauth(user_id=user_id, slug=slug, reason=reason)
            msg = "Accès Google révoqué ou expiré. Reconnecte Google pour ce projet."
        elif reason == "oauth_invalid_client":
            msg = "OAuth Google invalide (client id/secret). Vérifie la config plateforme."
        else:
            msg = f"AuthError: {type(e).__name__}: {e}"
        return JSONResponse({"ok": False, "error": msg, "reason": reason or "auth_failed"}, status_code=400)

    try:
        resp = requests.get(
            "https://searchconsole.googleapis.com/webmasters/v3/sites",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=30,
        )
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"RequestError: {type(e).__name__}: {e}"}, status_code=400)

    if resp.status_code != 200:
        reason, hint = _classify_gsc_api_failure(resp)
        info = _google_api_error_info(resp)
        if reason == "gsc_auth_failed":
            msg = "Accès Google refusé ou expiré."
        elif reason == "gsc_api_disabled":
            msg = "Search Console API indisponible côté Google Cloud."
        elif reason == "gsc_rate_limited":
            msg = "Quota Google atteint temporairement."
        elif reason == "gsc_insufficient_scope":
            msg = "Scopes Google insuffisants pour lire Search Console."
        else:
            msg = f"HTTP {resp.status_code}: {info.get('message') or 'gsc_request_failed'}"
        payload: dict[str, Any] = {"ok": False, "error": msg, "reason": reason}
        if hint:
            payload["hint"] = hint
        if info.get("message") and reason != "gsc_request_failed":
            payload["provider_error"] = info["message"]
        return JSONResponse(payload, status_code=400)

    try:
        data = resp.json()
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"JSONDecodeError: {e}"}, status_code=400)

    entries = data.get("siteEntry") if isinstance(data, dict) else None
    if not isinstance(entries, list):
        entries = []

    existing_by_base = _db_project_lookup_by_base_url(user_id)
    base_url = str(getattr(proj, "base_url", "") or "").strip()
    recommended: set[str] = set(_gsc_property_candidates(base_url, None)) if base_url else set()
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
        is_recommended = bool(site_url and site_url in recommended)
        props.append(
            {
                "property_url": site_url,
                "permission": perm,
                "domain": domain,
                "suggested_base_url": suggested,
                "is_recommended": is_recommended,
                "already_imported": bool(existing_slug),
                "project_slug": existing_slug or "",
            }
        )

    props.sort(
        key=lambda p: (
            0 if p.get("is_recommended") else 1,
            (p.get("domain") or p.get("property_url") or "").lower(),
        )
    )
    payload: dict[str, Any] = {"ok": True, "properties": props}
    if not props:
        payload["hint"] = "Aucune propriété Search Console vérifiée n’est accessible avec ce compte pour ce projet."
    return JSONResponse(payload)


@app.get("/api/projects/{slug}/bing/sites")
def bing_sites_for_project(request: Request, slug: str) -> JSONResponse:
    proj = _db_project_or_404(request, slug)
    user = getattr(request.state, "user", None)
    if not user:
        return JSONResponse({"ok": False, "error": "auth_required"}, status_code=401)

    user_id = str(getattr(user, "id", "") or "")
    auth = _effective_bing_connection(user_id=user_id)
    if not auth.get("token"):
        return JSONResponse({"ok": False, "error": "Bing non connecté pour ce compte."}, status_code=400)

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

    # Extract sites from payload (robust across payload shapes)
    sites: list[str] = []
    try:
        rows = _bing_extract_rows(data)
        for row in rows:
            for key in ("Url", "url", "SiteUrl", "siteUrl", "site_url"):
                value = row.get(key)
                if isinstance(value, str) and value.startswith(("http://", "https://")):
                    if value not in sites:
                        sites.append(value.strip())
                    break
        if not sites:
            blob = json.dumps(data, ensure_ascii=False)
            for u in re.findall(r"https?://[^\s\"\\\\]+", blob):
                if u not in sites:
                    sites.append(u)
    except Exception:
        pass

    base_url = str(getattr(proj, "base_url", "") or "").strip()
    existing_by_base = _db_project_lookup_by_base_url(user_id)
    candidates = {c.rstrip("/").lower() for c in _bing_site_candidates(base_url, None)} if base_url else set()
    host = (urlsplit(base_url).hostname or "").strip().lower()
    host_no_www = host[4:] if host.startswith("www.") else host

    def _score(site_url: str) -> int:
        root = _root_url(site_url).rstrip("/").lower()
        site_host = (urlsplit(site_url).hostname or "").lower()
        pts = 0
        if root in candidates:
            pts += 3
        if site_host and host and site_host == host:
            pts += 2
        if host_no_www and site_host == host_no_www:
            pts += 2
        return pts

    items: list[dict[str, Any]] = []
    for site_url in sites[:200]:
        norm = _normalize_base_url(site_url) or ""
        existing_slug = existing_by_base.get(norm)
        sc = _score(site_url)
        items.append({
            "site_url": site_url,
            "domain": (urlsplit(site_url).hostname or "").lower(),
            "is_recommended": sc > 0,
            "already_imported": bool(existing_slug),
            "project_slug": existing_slug or "",
        })

    items.sort(key=lambda x: (0 if x["is_recommended"] else 1, x["domain"]))
    payload: dict[str, Any] = {"ok": True, "sites": items}
    if not items:
        payload["hint"] = "Aucun site détecté dans ce compte Bing Webmaster Tools."
    return JSONResponse(payload)


@app.get("/api/gsc/properties")
def gsc_properties(request: Request) -> JSONResponse:
    _ = _require_system_owner(request)
    # Use _env_effective_value so credentials saved via the UI (DATA_DIR/.env.gsc) are found
    # even when os.environ wasn't updated (e.g. after a server restart on Render ephemeral disk).
    creds_raw, _cred_src = _env_effective_value("GOOGLE_APPLICATION_CREDENTIALS")
    creds = (creds_raw or "").strip().strip('"').strip("'")
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

    # Mark properties that are already added as projects for this user.
    # Non-fatal: if this lookup fails, still return the properties (without the
    # "already added" flag) instead of surfacing a generic 500 to the user.
    user = getattr(request.state, "user", None)
    existing_domains: set[str] = set()
    if user:
        try:
            with DB.session() as db:
                existing = list(db.scalars(select(Project).where(Project.owner_user_id == str(user.id))))
                for proj in existing:
                    h = (urlsplit(proj.base_url).hostname or "").lower().lstrip("www.")
                    if h:
                        existing_domains.add(h)
        except Exception as e:
            logger.warning("[GSC] existing-projects lookup failed: %s: %s", type(e).__name__, e)

    for p in props:
        d = (p.get("domain") or "").lower().lstrip("www.")
        p["already_added"] = d in existing_domains

    return JSONResponse({"ok": True, "properties": props})


@app.get("/api/gsc/properties/via-oauth")
def gsc_properties_via_oauth(request: Request) -> JSONResponse:
    """Return all GSC properties via the user's first connected OAuth project (not service account)."""
    user = getattr(request.state, "user", None)
    if not user:
        raise HTTPException(status_code=401, detail="auth_required")

    user_id = str(user.id)

    # Find first project with a valid OAuth refresh token
    with DB.session() as db:
        all_projects = list(
            db.scalars(select(Project).where(Project.owner_user_id == user_id).order_by(Project.site_name))
        )

    connected_slug: str | None = None
    for proj in all_projects:
        slug = str(getattr(proj, "slug", "") or "").strip()
        if slug and _gsc_oauth_connected(user_id, slug):
            rt = _gsc_oauth_refresh_token(user_id, slug)
            if rt:
                connected_slug = slug
                break

    if not connected_slug:
        return JSONResponse(
            {"ok": False, "error": "Aucun projet connecté à Google Search Console. Connecte un projet dans Paramètres → Comptes & connexions."},
            status_code=400,
        )

    client_id, client_secret = _google_oauth_client()
    if not client_id or not client_secret:
        return JSONResponse({"ok": False, "error": "Google OAuth non configuré (client id/secret)."}, status_code=400)

    refresh_token = _gsc_oauth_refresh_token(user_id, connected_slug)
    try:
        access_token = _google_oauth_refresh_access_token(
            refresh_token=refresh_token,
            client_id=client_id,
            client_secret=client_secret,
        )
    except Exception as e:
        reason = _classify_google_oauth_failure(e)
        if reason == "oauth_invalid_grant":
            _clear_stale_gsc_oauth(user_id=user_id, slug=connected_slug, reason=reason)
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
        return JSONResponse({"ok": False, "error": f"HTTP {resp.status_code}"}, status_code=400)

    try:
        data = resp.json()
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"JSONDecodeError: {e}"}, status_code=400)

    entries = data.get("siteEntry") if isinstance(data, dict) else None
    if not isinstance(entries, list):
        entries = []

    # Build existing project domains for already_added flag
    existing_domains: set[str] = set()
    try:
        with DB.session() as db:
            existing = list(db.scalars(select(Project).where(Project.owner_user_id == user_id)))
            for proj in existing:
                h = (urlsplit(proj.base_url).hostname or "").lower().lstrip("www.")
                if h:
                    existing_domains.add(h)
    except Exception as e:
        logger.warning("[GSC via-oauth] existing-projects lookup failed: %s: %s", type(e).__name__, e)

    # Deduplicate by domain (prefer sc-domain over URL-prefix)
    seen_domains: dict[str, dict[str, Any]] = {}
    for it in entries:
        if not isinstance(it, dict):
            continue
        site_url = str(it.get("siteUrl") or "").strip()
        perm = str(it.get("permissionLevel") or "").strip()
        if not site_url or perm.lower() in {"siteunverifieduser"}:
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

        d_key = domain.lower().lstrip("www.")
        if not d_key:
            continue
        # Prefer sc-domain entry; skip URL-prefix duplicates for same domain
        if d_key not in seen_domains or site_url.startswith("sc-domain:"):
            h = domain.lower().lstrip("www.")
            seen_domains[d_key] = {
                "property_url": site_url,
                "permission": perm,
                "domain": domain,
                "suggested_base_url": suggested,
                "already_added": h in existing_domains,
            }

    props = sorted(seen_domains.values(), key=lambda p: (p.get("domain") or "").lower())
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
                dimension_header: _csv_safe_value(str(it.get("keyword") or "")),
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
    # `query_page` returns the pairing, which is what makes a keyword opportunity actionable:
    # not "this query underperforms" but "THIS PAGE underperforms on this query". Google Search
    # Console only; Bing's reporting has no equivalent joint dimension here.
    allowed = {"query", "page", "query_page"} if source_key == "gsc" else {"query", "page"}
    if dimension not in allowed:
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

    # Fallback: if live returned ok but no items, try stored crawl CSV
    if payload.get("ok") and not payload.get("items"):
        runs_dir = _runs_dir_for_request(request)
        fallback_items = _crawl_items_fallback(runs_dir, slug, source_key, dimension, requested_limit)
        if fallback_items:
            payload = dict(payload)
            payload["items"] = fallback_items
            payload["live"] = False
            payload["fallback"] = True

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
    _, _, _, all_domains = _read_inventory_domains(Path(config_path)) if config_path else (None, ";", "domain", [])
    sites_rows = _parse_sites_summary_md(latest.get("sites_summary_md") if latest else None)

    try:
        cfg = yaml.safe_load(Path(config_path).read_text(encoding="utf-8")) if config_path else None
    except Exception:
        cfg = None
    defaults = cfg.get("defaults") if isinstance(cfg, dict) and isinstance(cfg.get("defaults"), dict) else {}
    crawl_defaults = defaults.get("crawl") if isinstance(defaults.get("crawl"), dict) else {}
    gsc_api_defaults = defaults.get("gsc_api") if isinstance(defaults.get("gsc_api"), dict) else {}

    gsc_creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    gsc_creds_exists = bool(gsc_creds and Path(gsc_creds).expanduser().exists())
    cron_secret_set = bool(os.environ.get("CRON_SECRET"))

    env_status = {
        "pagespeed_api_key_set": bool(os.environ.get("PAGESPEED_API_KEY")),
        "gsc_credentials_set": bool(gsc_creds),
        "gsc_credentials_exists": gsc_creds_exists,
        "cron_secret_set": cron_secret_set,
    }
    config_status = {
        "pagespeed_enabled": bool(crawl_defaults.get("pagespeed") or False),
        "gsc_api_enabled": bool(gsc_api_defaults.get("enabled") or False),
    }

    all_jobs = _list_jobs(limit=50)
    autopilot_jobs = [j for j in all_jobs if _job_kind_from_command(j.command) == "autopilot"]
    current_job = next((j for j in autopilot_jobs if j.status in {"queued", "running"}), None)

    resp = templates.TemplateResponse(
        "automation.html",
        {
            "request": request,
            "repo_root": str(REPO_ROOT),
            "config_path": str(config_path) if config_path else None,
            "runs_dir": str(runs_dir),
            "latest": latest,
            "jobs": autopilot_jobs[:12],
            "current_job": {"id": current_job.id, "status": current_job.status} if current_job else None,
            "inventory": inventory,
            "all_domains": all_domains,
            "sites_rows": sites_rows,
            "env_status": env_status,
            "config_status": config_status,
        },
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.get("/api/automation/domains")
def api_automation_domains_get(request: Request) -> JSONResponse:
    _ = _require_admin(request)
    config_path = DEFAULT_CONFIG if DEFAULT_CONFIG.exists() else None
    if not config_path:
        return JSONResponse({"error": "yml manquant"}, status_code=404)
    csv_path_str, _, _, domains = _read_inventory_domains(Path(config_path))
    return JSONResponse({"csv_path": csv_path_str, "domains": domains, "count": len(domains)})


class _DomainsBody(BaseModel):
    domains: list[str]


@app.post("/api/automation/domains")
def api_automation_domains_save(request: Request, body: _DomainsBody) -> JSONResponse:
    _ = _require_admin(request)
    config_path = DEFAULT_CONFIG if DEFAULT_CONFIG.exists() else None
    if not config_path:
        return JSONResponse({"error": "yml manquant"}, status_code=400)
    csv_path_str, delimiter, domain_col, _ = _read_inventory_domains(Path(config_path))
    if not csv_path_str:
        return JSONResponse({"error": "domains_csv non défini dans le yml"}, status_code=400)
    csv_path = Path(csv_path_str)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    seen: set[str] = set()
    unique: list[str] = []
    for d in body.domains:
        d = d.strip()
        if d and d not in seen:
            seen.add(d)
            unique.append(d)
    try:
        with csv_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[domain_col], delimiter=delimiter)
            writer.writeheader()
            for d in unique:
                writer.writerow({domain_col: d})
        return JSONResponse({"ok": True, "count": len(unique)})
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.get("/api/automation/github-corrections")
def api_automation_github_corrections(request: Request) -> JSONResponse:
    _ = _require_admin(request)
    user = getattr(request.state, "user", None)
    if not user:
        return JSONResponse({"ok": False, "error": "Non connecté."}, status_code=401)
    result: list[dict[str, Any]] = []
    try:
        with DB.session() as db:
            projects = list(db.scalars(
                select(Project).where(Project.owner_user_id == str(user.id)).order_by(Project.site_name)
            ))
            for proj in projects:
                cfg = _project_github_cfg(proj)
                counts: dict[str, int] = {"todo": 0, "in_progress": 0, "done": 0, "ignored": 0}
                if cfg["repo"]:
                    tasks_raw = list(db.scalars(
                        select(IssueTask)
                        .where(IssueTask.project_id == proj.id)
                        .order_by(IssueTask.updated_at.desc())
                    ))
                    for t in tasks_raw:
                        st = str(t.status or "todo")
                        counts[st] = counts.get(st, 0) + 1
                result.append({
                    "slug": str(proj.slug),
                    "site_name": str(proj.site_name or proj.slug),
                    "github_repo": cfg["repo"],
                    "github_mode": cfg["mode"],
                    "github_branch": cfg["branch"],
                    "counts": counts,
                })
    except Exception as exc:
        import traceback
        print(f"[api_automation_github_corrections] {exc}\n{traceback.format_exc()}", flush=True)
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)
    return JSONResponse({"ok": True, "projects": result})


@app.get("/cron/autopilot")
def cron_autopilot(request: Request, background_tasks: BackgroundTasks) -> JSONResponse:
    cron_secret = str(os.environ.get("CRON_SECRET") or "").strip()
    if not cron_secret:
        return JSONResponse({"ok": False, "error": "CRON_SECRET non configuré"}, status_code=500)
    auth = request.headers.get("Authorization", "")
    token = auth[len("Bearer "):].strip() if auth.startswith("Bearer ") else auth.strip()
    if not token or not hmac.compare_digest(token, cron_secret):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    config_path = DEFAULT_CONFIG if DEFAULT_CONFIG.exists() else None
    if not config_path:
        return JSONResponse({"ok": False, "error": "yml manquant"}, status_code=500)
    extra_args = ["--mode", "audit-only", "--no-auto-deploy", "--no-backlog"]
    script = REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_autopilot.py"
    job = Job(id=str(uuid.uuid4()), status="queued", created_at=time.time(), config_path=str(config_path))
    job.command = [sys.executable, "-u", str(script), "--config", str(config_path)] + extra_args
    job.result = {"type": "autopilot", "user_id": "cron", "run_policy": "verify"}
    _save_job(job)
    background_tasks.add_task(_run_autopilot_job, job.id, config_path, extra_args)
    return JSONResponse({"ok": True, "job_id": job.id})


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
        ts_label = (run_dt.strftime("%d/%m/%y %H:%M") if run_dt else None) or (
            created_dt.strftime("%d/%m/%y %H:%M") if created_dt else ""
        )
        sa = float(j.started_at or j.created_at or 0)
        fa = float(j.finished_at or 0)
        dur_s = int(fa - sa) if fa > sa else None
        jobs_view.append(
            {
                "id": j.id,
                "status": j.status,
                "kind": kind,
                "slug": slug,
                "timestamp": run_ts,
                "timestamp_label": ts_label,
                "created_at": j.created_at,
                "started_at": j.started_at,
                "finished_at": j.finished_at,
                "duration_label": _fmt_duration(dur_s) if dur_s is not None else None,
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
    max_bytes = _file_view_max_bytes()
    try:
        size = raw_path.stat().st_size
    except Exception:
        size = 0
    if size > max_bytes:
        return HTMLResponse(f"File too large to preview ({size} bytes, max {max_bytes})", status_code=413)

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
    try:
        cfg = _resolve_request_config_path(request, config_path)
    except HTTPException:
        return RedirectResponse(
            url=_path_with_flash("/automation", err="Fichier de configuration refusé."),
            status_code=303,
        )
    if not cfg.exists():
        return RedirectResponse(
            url=_path_with_flash("/automation", err="Fichier de configuration introuvable."),
            status_code=303,
        )

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
    try:
        cfg = _resolve_request_config_path(request, config_path)
    except HTTPException as exc:
        msg = "Fichier de configuration refusé."
        if _client_wants_json(request):
            return JSONResponse({"ok": False, "error": msg}, status_code=int(exc.status_code or 403))
        return RedirectResponse(url=f"/projects/{slug}?err={quote(msg)}", status_code=303)

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
            # The scarce resource is worker slot-time, not the monthly page quota. Past the
            # plan's max_pages_per_crawl the job provably cannot finish before its timeout:
            # it would hold a slot for hours, die, and teach the user nothing except to retry.
            # Clamp the request up front so the crawl that starts is one that can end.
            plan_crawl = billing.crawl_config_for_plan(
                billing.effective_plan_key(db, user_id=str(getattr(user, "id", "")))
            )
            plan_max_pages = int(plan_crawl.get("max_pages_per_crawl") or 0)
            if plan_max_pages > 0 and planned_pages > plan_max_pages:
                planned_pages = plan_max_pages
                override_max_pages = plan_max_pages
            plan_timeout_s = int(plan_crawl.get("job_timeout_s") or 0)
            if plan_timeout_s > 0:
                job.result["job_timeout_s"] = plan_timeout_s
            plan_ps_urls = int(plan_crawl.get("max_pagespeed_urls") or 0)
            if plan_ps_urls > 0:
                job.result["max_pagespeed_urls"] = plan_ps_urls

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
    try:
        cfg = _resolve_request_config_path(request, config_path)
    except HTTPException as exc:
        msg = "Fichier de configuration refusé."
        if _client_wants_json(request):
            return JSONResponse({"ok": False, "error": msg}, status_code=int(exc.status_code or 403))
        return RedirectResponse(url=_path_with_flash("/", err=msg), status_code=303)

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
    gsc_status = _gsc_live_credentials_status(user_id=str(getattr(user, "id", "")), slug=slug)
    live_series = {
        "gsc": {
            "enabled": bool(effective_gsc.get("enabled")) if "enabled" in effective_gsc else True,
            "days": int(effective_gsc.get("days") or 28),
            "credentials_ready": bool(gsc_status.get("ready")),
            "auth_mode": str(gsc_status.get("auth_mode") or ""),
            "reason": str(gsc_status.get("reason") or ""),
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
    crawl_items: dict[str, dict[str, list[dict[str, Any]]]] = {"gsc": {"query": [], "page": []}, "bing": {"query": [], "page": []}}
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
            meta = report.get("meta") if isinstance(report.get("meta"), dict) else {}
            for src_key in ("gsc", "bing"):
                if src_key == "gsc":
                    src_meta = meta.get("gsc_api") if isinstance(meta.get("gsc_api"), dict) else {}
                else:
                    src_meta = meta.get("bing") if isinstance(meta.get("bing"), dict) else {}
                if not src_meta.get("ok"):
                    continue
                for dim_key in ("query", "page"):
                    csv_key = "queries_csv" if dim_key == "query" else "pages_csv"
                    csv_path_str = str(src_meta.get(csv_key) or "").strip()
                    if not csv_path_str:
                        continue
                    csv_path = Path(csv_path_str)
                    if not csv_path.exists():
                        _ensure_runs_artifact_local(csv_path)
                    if not csv_path.exists():
                        continue
                    try:
                        rows = _read_gsc_csv_rows(csv_path)
                        rows.sort(key=lambda r: (-_to_int(r.get("clicks")), -_to_int(r.get("impressions"))))
                        crawl_items[src_key][dim_key] = rows[:12]
                    except Exception:
                        pass
    except Exception as e:
        print(f"[OVERVIEW] crawl items error: {type(e).__name__}: {e}")

    crawl_timing = _crawl_timing_map(slug)
    for h in (data.get("history") or []):
        t = crawl_timing.get(str(h.get("timestamp") or ""), {})
        h["duration_s"] = t.get("duration_s")
        h["duration_label"] = t.get("duration_label")

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
            "crawl_items": crawl_items,
            "crawl_timing": crawl_timing,
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
    # Show the plan's real per-crawl ceiling in the form. Offering 200 000 to everyone meant
    # the limit was only ever discovered by a crawl that ran for hours and then died.
    if bool(getattr(user, "is_admin", False)):
        plan_crawl = {"max_pages_per_crawl": 200_000, "job_timeout_s": 0, "max_pagespeed_urls": 1_000}
    else:
        with DB.session() as db:
            plan_crawl = billing.crawl_config_for_plan(
                billing.effective_plan_key(db, user_id=str(getattr(user, "id", "")))
            )
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
            "plan_crawl": plan_crawl,
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
    pagespeed_workers: int = Form(default=6),
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
        _ensure_runs_file_local(fix_path_obj)
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
    # Load existing GitHub PR tasks for this issue (keyed by URL)
    gh_tasks: dict[str, dict[str, Any]] = {}
    try:
        with DB.session() as _db:
            _tasks = list(_db.scalars(
                select(IssueTask).where(
                    IssueTask.project_id == proj_row.id,
                    IssueTask.issue_key == issue_key,
                )
            ))
        for _t in _tasks:
            if not _t.url:
                continue
            try:
                _note_data = json.loads(_t.note) if _t.note else {}
            except Exception:
                _note_data = {}
            _verify = _note_data.get("verify") if isinstance(_note_data, dict) else None
            gh_tasks[_t.url] = {
                "status": _t.status,
                "pr": _note_data,
                "verify": _verify if isinstance(_verify, dict) else None,
            }
    except Exception:
        pass

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
            "gh_tasks": gh_tasks,
        },
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.get("/api/projects/{slug}/issues/{issue_key}/url-fix")
def api_issue_url_fix(
    request: Request,
    slug: str,
    issue_key: str,
    url: str = "",
    crawl: str | None = None,
) -> JSONResponse:
    proj_row = _db_project_or_404(request, slug)
    user = getattr(request.state, "user", None)
    retry_after = _rate_limit_retry_after(
        bucket="issue_url_fix_user",
        subject=str(getattr(user, "id", "")),
        limit=30,
        window_s=60 * 60,
    )
    if isinstance(retry_after, int):
        return JSONResponse(
            {"ok": False, "error": f"Trop de requêtes. Réessaie dans {_format_retry_after(retry_after)}."},
            status_code=429,
            headers={"Retry-After": str(retry_after)},
        )
    url = (url or "").strip()
    url_error = _validate_settings_url(url)
    if url_error:
        return JSONResponse({"ok": False, "error": url_error}, status_code=400)
    gate_ok, gate_msg, _gmax, gate_model = _correction_gate(user)
    if not gate_ok:
        return JSONResponse({"error": gate_msg, "billing_url": "/billing"}, status_code=402)
    meta = dash.issue_meta(issue_key)
    errors: list[str] = []
    result = _openai_url_fix(
        issue_key=issue_key,
        issue_label=meta.label if meta else issue_key,
        url=url,
        site_name=str(proj_row.site_name or slug),
        error_sink=errors,
        model_override=gate_model,
    )
    if not result:
        # System/provider details are admin-only; regular users get a generic message.
        if bool(getattr(user, "is_admin", False)) and errors:
            msg = "Correction IA indisponible : " + " · ".join(errors[:2])
        else:
            msg = "Correction IA momentanément indisponible. Réessaie dans un instant."
        return JSONResponse({"error": msg}, status_code=503)
    _correction_charge(user, 1)
    return JSONResponse(result)


class _GithubConnectBody(BaseModel):
    repo: str        # "owner/repo"
    branch: str = "main"
    mode: str = "review"   # "review" or "auto"


class _GithubFixBody(BaseModel):
    url: str
    crawl_ts: str = ""
    confirm: bool = False
    file_path: str = ""
    patched_content: str = ""


@app.post("/api/projects/{slug}/github/connect")
def api_github_connect(request: Request, slug: str, body: _GithubConnectBody) -> JSONResponse:
    proj = _db_project_or_404(request, slug)
    user = getattr(request.state, "user", None)
    token, source = _effective_user_connection_value(user_id=str(user.id), key="GITHUB_TOKEN")
    if not token or source != "user":
        return JSONResponse({"ok": False, "error": "GitHub non connecté. Va dans Comptes & connexions pour connecter GitHub."}, status_code=400)
    repo = (body.repo or "").strip()
    repo_parts = _github_repo_parts(repo)
    if repo_parts is None:
        return JSONResponse({"ok": False, "error": "Format invalide. Utilise owner/repo."}, status_code=400)
    owner, repo_name = repo_parts
    repo = f"{owner}/{repo_name}"
    try:
        repo_info = _github_api_get(_github_api_path("repos", owner, repo_name), token=token, timeout_s=10)
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Dépôt introuvable ou inaccessible : {e}"}, status_code=400)
    perms = repo_info.get("permissions") if isinstance(repo_info.get("permissions"), dict) else {}
    if not perms.get("push"):
        return JSONResponse({"ok": False, "error": "Ton token GitHub n'a pas les droits d'écriture sur ce dépôt. Utilise un token avec scope 'repo'."}, status_code=400)
    mode = body.mode if body.mode in ("review", "auto") else "review"
    branch = (body.branch or repo_info.get("default_branch") or "main").strip()
    if not _github_branch_allowed(branch):
        return JSONResponse({"ok": False, "error": "Branche GitHub invalide."}, status_code=400)
    settings = dict(proj.settings) if isinstance(proj.settings, dict) else {}
    settings["github_repo"] = repo
    settings["github_branch"] = branch
    settings["github_mode"] = mode
    with DB.session() as db:
        p = db.get(type(proj), proj.id)
        if p:
            p.settings = settings
            db.commit()
    return JSONResponse({"ok": True, "repo": repo, "branch": branch, "mode": mode, "html_url": str(repo_info.get("html_url") or "")})


@app.get("/api/projects/{slug}/github/status")
def api_github_status(request: Request, slug: str) -> JSONResponse:
    proj = _db_project_or_404(request, slug)
    cfg = _project_github_cfg(proj)
    return JSONResponse({"ok": True, **cfg, "connected": bool(cfg["repo"])})


@app.post("/api/projects/{slug}/issues/{issue_key}/github-fix")
def api_github_fix(request: Request, slug: str, issue_key: str, body: _GithubFixBody) -> JSONResponse:
    proj = _db_project_or_404(request, slug)
    user = getattr(request.state, "user", None)
    cfg = _project_github_cfg(proj)
    if not cfg["repo"]:
        return JSONResponse({"ok": False, "needs_setup": True, "slug": slug, "error": "Aucun dépôt GitHub connecté à ce projet."}, status_code=400)
    token, source = _effective_user_connection_value(user_id=str(user.id), key="GITHUB_TOKEN")
    if not token or source != "user":
        return JSONResponse({"ok": False, "error": "GitHub non connecté."}, status_code=400)
    retry_after = _rate_limit_retry_after(
        bucket="github_fix_user",
        subject=str(getattr(user, "id", "")),
        limit=20,
        window_s=60 * 60,
    )
    if isinstance(retry_after, int):
        return JSONResponse(
            {"ok": False, "error": f"Trop de requêtes. Réessaie dans {_format_retry_after(retry_after)}."},
            status_code=429,
            headers={"Retry-After": str(retry_after)},
        )
    repo_parts = _github_repo_parts(cfg["repo"])
    if repo_parts is None:
        return JSONResponse({"ok": False, "needs_setup": True, "error": "Configuration GitHub invalide."}, status_code=400)
    owner, repo_name = repo_parts
    branch = cfg["branch"]
    if not _github_branch_allowed(branch):
        return JSONResponse({"ok": False, "needs_setup": True, "error": "Branche GitHub invalide."}, status_code=400)
    mode = cfg["mode"]
    gate_ok, gate_msg, _gmax, gate_model = _correction_gate(user)
    if not gate_ok:
        return JSONResponse({"ok": False, "error": gate_msg, "billing_url": "/billing"}, status_code=402)
    url = (body.url or "").strip()
    if not url:
        return JSONResponse({"ok": False, "error": "URL manquante."}, status_code=400)
    url_error = _validate_settings_url(url)
    if url_error:
        return JSONResponse({"ok": False, "error": url_error}, status_code=400)
    meta = dash.issue_meta(issue_key)
    issue_label = meta.label if meta else issue_key
    site_name = str(proj.site_name or slug)

    # ── Step 2: Apply the fix (create branch + commit + PR) ──────────────
    if body.confirm:
        import base64 as _b64
        file_path = (body.file_path or "").strip()
        if not _github_file_path_allowed(file_path):
            return JSONResponse({"ok": False, "error": "Chemin de fichier GitHub invalide."}, status_code=400)
        content_error = _github_patched_content_error(body.patched_content)
        if content_error:
            return JSONResponse({"ok": False, "error": content_error}, status_code=400)
        try:
            ref_data = _github_api_get(_github_ref_api_path(owner, repo_name, branch), token=token)
            base_sha = ref_data["object"]["sha"]
        except Exception as e:
            return JSONResponse({"ok": False, "error": f"Impossible de lire la branche {branch} : {e}"}, status_code=400)
        from datetime import datetime as _dt
        fix_branch = f"seo-fix/{_safe_github_branch_suffix(issue_key)}-{_dt.utcnow().strftime('%Y%m%d-%H%M%S')}"
        try:
            _github_api_post(_github_api_path("repos", owner, repo_name, "git", "refs"), token=token, json_body={
                "ref": f"refs/heads/{fix_branch}",
                "sha": base_sha,
            })
        except Exception as e:
            return JSONResponse({"ok": False, "error": f"Impossible de créer la branche {fix_branch} : {e}"}, status_code=400)
        try:
            file_data = _github_api_get(_github_content_api_path(owner, repo_name, file_path), token=token, params={"ref": branch})
            current_sha = file_data.get("sha", "")
        except Exception:
            current_sha = ""
        encoded = _b64.b64encode(body.patched_content.encode("utf-8")).decode("ascii")
        commit_msg = f"fix(seo): correct {issue_key} on {url[:80]}\n\nGenerated by SEO Agent — {site_name}"
        put_body: dict[str, Any] = {
            "message": commit_msg,
            "content": encoded,
            "branch": fix_branch,
        }
        if current_sha:
            put_body["sha"] = current_sha
        commit_sha = ""
        commit_url = ""
        try:
            put_resp = _github_api_put(_github_content_api_path(owner, repo_name, file_path), token=token, json_body=put_body)
            commit_sha = str(put_resp.get("commit", {}).get("sha") or "")
            commit_url = str(put_resp.get("commit", {}).get("html_url") or "")
        except Exception as e:
            return JSONResponse({"ok": False, "error": f"Impossible de committer la correction : {e}"}, status_code=400)
        pr_title = f"fix(seo): {issue_label} — {url[:60]}"
        pr_body = (
            f"## Correction SEO automatique\n\n"
            f"**Anomalie :** {issue_label} (`{issue_key}`)\n"
            f"**URL affectée :** {url}\n"
            f"**Fichier modifié :** `{file_path}`\n\n"
            f"Correction générée par [SEO Agent](https://noyaru.com) pour **{site_name}**.\n\n"
            f"> Vérifie les changements avant de merger."
        )
        try:
            pr_data = _github_api_post(_github_api_path("repos", owner, repo_name, "pulls"), token=token, json_body={
                "title": pr_title,
                "body": pr_body,
                "head": fix_branch,
                "base": branch,
            })
            pr_url = pr_data.get("html_url", "")
            pr_number = pr_data.get("number", "")
        except Exception as e:
            return JSONResponse({"ok": False, "error": f"Erreur lors de la création de la PR : {e}"}, status_code=400)
        # Auto-record the PR as an IssueTask so the issue detail page shows it
        try:
            _pr_note = json.dumps({
                "pr_url": pr_url, "pr_title": pr_title,
                "pr_number": int(pr_number) if pr_number else 0,
                "commit_sha": commit_sha[:7] if commit_sha else "",
                "commit_url": commit_url, "branch": fix_branch, "file": file_path,
            }, ensure_ascii=False)
            with DB.session() as _db2:
                _existing = _db2.scalar(select(IssueTask).where(
                    IssueTask.project_id == proj.id,
                    IssueTask.issue_key == issue_key,
                    IssueTask.url == url,
                ))
                if _existing:
                    _existing.status = "in_progress"
                    _existing.note = _pr_note
                    _existing.issue_label = issue_label
                else:
                    _db2.add(IssueTask(
                        project_id=str(proj.id),
                        user_id=str(getattr(user, "id", "") or ""),
                        issue_key=issue_key, issue_label=issue_label,
                        crawl_ts=str(body.crawl_ts or ""), url=url,
                        status="in_progress", severity=str(
                            (dash.issue_meta(issue_key).severity if dash.issue_meta(issue_key) else None) or "notice"
                        ),
                        note=_pr_note,
                    ))
                _db2.commit()
        except Exception:
            pass
        # No auto-merge here, even in Full Access mode: this endpoint commits a `patched_content`
        # produced by the model, so the diff is always an editorial proposal and a human has to
        # read it. Only the deterministic families (deep-fix with a bounded rewriter) still merge
        # on their own — the PR is created either way, the task simply stays in_progress.
        _merged = False
        return JSONResponse({
            "ok": True,
            "pr_url": pr_url,
            "pr_title": pr_title,
            "pr_number": pr_number,
            "branch": fix_branch,
            "commit_sha": commit_sha[:7] if commit_sha else "",
            "commit_url": commit_url,
            "file": file_path,
            "merged": _merged,
        })

    # ── Step 1: Find file + generate patch (preview) ─────────────────────
    try:
        seo_files = _github_find_seo_files(owner, repo_name, branch, token, issue_key)
    except RuntimeError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    best = seo_files[0]
    patch = _openai_generate_file_patch(
        file_path=best["path"],
        file_content=best["content"],
        issue_key=issue_key,
        issue_label=issue_label,
        url=url,
        site_name=site_name,
        model_override=gate_model,
    )
    if not patch:
        return JSONResponse({"ok": False, "error": "Service IA indisponible ou réponse invalide."}, status_code=503)
    if patch.get("error"):
        return JSONResponse(
            {"ok": False, "error": str(patch.get("description") or patch.get("error"))},
            status_code=422,
        )
    content_error = _github_patched_content_error(str(patch.get("patched_content") or ""))
    if content_error:
        return JSONResponse({"ok": False, "error": content_error}, status_code=400)
    _correction_charge(user, 1)

    # In auto mode: apply immediately without confirm step
    if mode == "auto":
        auto_body = _GithubFixBody(
            url=url, crawl_ts=body.crawl_ts,
            confirm=True,
            file_path=best["path"],
            patched_content=patch["patched_content"],
        )
        return api_github_fix(request, slug, issue_key, auto_body)

    # Review mode: return preview
    original_lines = best["content"].splitlines()
    patched_lines = patch["patched_content"].splitlines()
    return JSONResponse({
        "ok": True,
        "mode": "review",
        "file": best["path"],
        "pr_title": patch.get("pr_title", f"fix(seo): {issue_label}"),
        "description": patch.get("description", ""),
        "original_preview": "\n".join(original_lines[:30]),
        "patched_preview": "\n".join(patched_lines[:30]),
        "patched_content": patch["patched_content"],
    })


@app.post("/api/projects/{slug}/github/bulk-fix")
def api_github_bulk_fix(request: Request, slug: str) -> JSONResponse:
    """Generate and push fixes for all crawl errors in a single PR."""
    proj = _db_project_or_404(request, slug)
    user = getattr(request.state, "user", None)
    cfg = _project_github_cfg(proj)
    if not cfg["repo"]:
        return JSONResponse({"ok": False, "needs_setup": True, "error": "Aucun dépôt GitHub connecté à ce projet."}, status_code=400)
    token, source = _effective_user_connection_value(user_id=str(user.id), key="GITHUB_TOKEN")
    if not token or source != "user":
        return JSONResponse({"ok": False, "error": "GitHub non connecté."}, status_code=400)
    retry_after = _rate_limit_retry_after(
        bucket="github_bulk_fix_user",
        subject=str(getattr(user, "id", "")),
        limit=5,
        window_s=60 * 60,
    )
    if isinstance(retry_after, int):
        return JSONResponse(
            {"ok": False, "error": f"Trop de requêtes. Réessaie dans {_format_retry_after(retry_after)}."},
            status_code=429,
            headers={"Retry-After": str(retry_after)},
        )
    gate_ok, gate_msg, gate_budget, gate_model = _correction_gate(user)
    if not gate_ok:
        return JSONResponse({"ok": False, "error": gate_msg, "billing_url": "/billing"}, status_code=402)
    repo_parts = _github_repo_parts(cfg["repo"])
    if repo_parts is None:
        return JSONResponse({"ok": False, "needs_setup": True, "error": "Configuration GitHub invalide."}, status_code=400)
    owner, repo_name = repo_parts
    branch = cfg["branch"]
    if not _github_branch_allowed(branch):
        return JSONResponse({"ok": False, "needs_setup": True, "error": "Branche GitHub invalide."}, status_code=400)
    mode = cfg["mode"]

    # Load latest crawl with a report
    runs_dir = _runs_dir_for_request(request)
    crawls = dash.list_project_crawls(runs_dir, slug)
    ts = next((t for t in reversed(crawls) if dash.load_report_json(runs_dir, slug, t)), None)
    if not ts:
        return JSONResponse({"ok": False, "error": "Aucun rapport de crawl disponible."}, status_code=400)
    report = dash.load_report_json(runs_dir, slug, ts)
    if not report:
        return JSONResponse({"ok": False, "error": "Rapport de crawl introuvable."}, status_code=400)

    site_name = str(proj.site_name or slug)

    # Keep this capped: each item can trigger GitHub + LLM calls.
    fixable = _github_fixable_issue_candidates(report=report, proj=proj, limit=5)

    if not fixable:
        return JSONResponse({"ok": False, "error": "Aucune erreur corrigeable trouvée dans le dernier crawl."}, status_code=400)

    # Create fix branch
    try:
        ref_data = _github_api_get(_github_ref_api_path(owner, repo_name, branch), token=token)
        base_sha = ref_data["object"]["sha"]
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Impossible de lire la branche {branch} : {e}"}, status_code=400)

    from datetime import datetime as _dt
    fix_branch = f"seo-fix/bulk-{_dt.utcnow().strftime('%Y%m%d-%H%M%S')}"
    try:
        _github_api_post(_github_api_path("repos", owner, repo_name, "git", "refs"), token=token, json_body={
            "ref": f"refs/heads/{fix_branch}", "sha": base_sha,
        })
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Impossible de créer la branche : {e}"}, status_code=400)

    # Read the repo tree once for AI file mapping.
    try:
        tree_data = _github_api_get(_github_api_path("repos", owner, repo_name, "git", "trees", branch), token=token, params={"recursive": "1"}, timeout_s=20)
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Lecture du dépôt impossible : {e}"}, status_code=400)
    all_paths = [
        item["path"] for item in (tree_data.get("tree") or [])
        if isinstance(item, dict) and item.get("type") == "blob" and _github_file_path_allowed(str(item.get("path") or ""))
    ]
    report_issues = report.get("issues") if isinstance(report.get("issues"), dict) else {}
    idx = repo_index.build_repo_index(all_paths)
    logger.info("[corrections] bulk-fix %s", repo_index.index_summary(idx))

    # Process each issue with deep coverage (multiple files/issue); share file_state so a
    # file touched by several issues stacks correctly.
    file_state: dict[str, dict[str, str]] = {}  # path → {sha, content}
    results: list[dict[str, Any]] = []
    config_changed: list[str] = []   # deterministic redirect-config repairs, never AI-generated
    config_notes: list[str] = []
    any_ai_written = False           # one model-written file is enough to require a human read
    ai_billable = 0                  # …but only the model-written ones are billed
    any_premise_key = ""             # …and so is one fix that rests on a debatable assumption
    budget = int(gate_budget)  # total files we may patch this run (plan cap ∩ remaining quota)
    for issue in fixable:
        if budget <= 0:
            break
        issue_key = issue["key"]
        issue_label = issue["label"]
        url = issue["url"]
        impacted = sorted(dash.extract_impacted_pages(issue_key, report_issues.get(issue_key))) if report_issues else []
        # Same preparation as the per-issue button: evidence, family hint, and the
        # deterministic rewriter. Without it this path silently ran a free-form AI patch for
        # every family, including on the routing config.
        _prep = _prepare_issue_fix(
            issue_key=issue_key, issues=report_issues, impacted=impacted, all_paths=all_paths,
            site_name=site_name, owner=owner, repo_name=repo_name, branch=branch, token=token,
            model_override=gate_model,
        )
        if _prep["refusal"]:
            results.append({"issue_key": issue_key, "issue_label": issue_label, "url": url,
                            "ok": False, "error": _prep["refusal"]})
            continue
        _cfg_changed: list[str] = []
        _cfg_notes: list[str] = []
        if _prep["loop_paths"]:
            try:
                _cfg_changed, _cfg_notes = _deep_fix_redirect_config_loops(
                    owner=owner, repo_name=repo_name, token=token, fix_branch=fix_branch,
                    all_paths=all_paths, loop_paths=_prep["loop_paths"][:6], file_state=file_state,
                )
            except Exception:
                _cfg_changed, _cfg_notes = [], []
            config_changed.extend(_cfg_changed)
            config_notes.extend(_cfg_notes)
        if issue_key in _REDIRECT_CONFIG_KEYS:
            # Config-only family: the rule prune above IS the repair; never let the content
            # patcher near netlify.toml / next.config from a prompt.
            results.append({"issue_key": issue_key, "issue_label": issue_label, "url": url,
                            "ok": bool(_cfg_changed), "files": _cfg_changed})
            continue
        patched, skipped, targets, _ai_files = _deep_patch_issue_files(
            owner=owner, repo_name=repo_name, branch=branch, token=token, fix_branch=fix_branch,
            all_paths=all_paths, issue_key=issue_key, issue_label=issue_label, impacted_urls=impacted,
            site_name=site_name, file_state=file_state, max_files=min(6, budget),
            evidence=_prep["evidence"], extra_hint=_prep["extra_hint"], model_override=gate_model,
            index=idx, link_rewriter=_prep["link_rewriter"],
            rewriter_ai_fallback=_prep["rewriter_ai_fallback"],
            rewriter_is_ai=bool(_prep["rewriter_is_ai"]),
        )
        any_ai_written = any_ai_written or bool(_ai_files)
        ai_billable += len(_ai_files)
        if not any_premise_key and _fix_premise_note(issue_key):
            any_premise_key = issue_key
        patched = patched + [f for f in _cfg_changed if f not in patched]
        if patched:
            budget -= len(patched)
            results.append({"issue_key": issue_key, "issue_label": issue_label, "url": url, "ok": True, "files": patched})
        else:
            results.append({"issue_key": issue_key, "issue_label": issue_label, "url": url, "ok": False,
                            "error": ("Aucun fichier corrigeable trouvé" if not targets else "Aucun patch appliqué")})

    fixed_results = [r for r in results if r.get("ok")]
    if not fixed_results:
        return JSONResponse({"ok": False, "error": "Aucune correction n'a pu être appliquée.", "results": results}, status_code=500)

    # Build PR
    _files_total = sum(len(r.get("files") or []) for r in fixed_results)
    pr_title = f"fix(seo): {len(fixed_results)} anomalie(s) corrigée(s) — {site_name}"
    pr_lines = [
        "## Corrections SEO automatiques — audit complet\n",
        f"**Site :** {site_name}  \n**Crawl :** `{ts}`  \n"
        f"**Anomalies corrigées :** {len(fixed_results)} · **Fichiers modifiés :** {_files_total}\n",
    ]
    for r in results:
        icon = "✅" if r.get("ok") else "❌"
        files = r.get("files") or []
        detail = ", ".join(f"`{f}`" for f in files) if files else (r.get("error") or "")
        pr_lines.append(f"{icon} **{r['issue_label']}** — {detail}")
    pr_lines.append(_fix_nature_note(any_ai_written, any_premise_key).strip())
    pr_lines.append("\nCorrection générée par [SEO Agent](https://noyaru.com).")
    pr_body = "\n".join(pr_lines)

    try:
        pr_data = _github_api_post(_github_api_path("repos", owner, repo_name, "pulls"), token=token, json_body={
            "title": pr_title, "body": pr_body, "head": fix_branch, "base": branch,
        })
        pr_url = pr_data.get("html_url", "")
        pr_number = pr_data.get("number", 0)
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Erreur PR : {e}", "results": results}, status_code=400)

    # Auto-merge if Full Access mode — except when the run touched redirect rules. Routing
    # changes always go through a human, exactly as the per-issue path already required.
    _merged = False
    if mode == "auto" and pr_number and not config_changed and not any_ai_written and not any_premise_key:
        try:
            _github_api_put(
                _github_api_path("repos", owner, repo_name, "pulls", str(int(pr_number)), "merge"),
                token=token,
                json_body={"merge_method": "squash", "commit_title": pr_title},
            )
            _merged = True
        except Exception:
            pass

    # Save IssueTask records for each successful fix
    final_status = "done" if _merged else "in_progress"
    pr_note_base = {"pr_url": pr_url, "pr_number": int(pr_number), "branch": fix_branch, "bulk": True, "deep": True}
    for r in fixed_results:
        try:
            _note = json.dumps({**pr_note_base, "files": r.get("files", [])}, ensure_ascii=False)
            _meta = dash.issue_meta(r["issue_key"])
            with DB.session() as _db:
                _ex = _db.scalar(select(IssueTask).where(
                    IssueTask.project_id == proj.id,
                    IssueTask.issue_key == r["issue_key"],
                    IssueTask.url == r["url"],
                ))
                if _ex:
                    _ex.status = final_status
                    _ex.note = _note
                else:
                    _db.add(IssueTask(
                        project_id=str(proj.id),
                        user_id=str(getattr(user, "id", "") or ""),
                        issue_key=r["issue_key"], issue_label=r["issue_label"],
                        crawl_ts=ts, url=r["url"], status=final_status,
                        severity=str((_meta.severity if _meta else None) or "notice"),
                        note=_note,
                    ))
                _db.commit()
        except Exception:
            pass

    # Bill all files patched across the bulk run (1 per file = 1 AI call).
    _correction_charge(user, ai_billable)

    return JSONResponse({
        "ok": True,
        "pr_url": pr_url, "pr_number": pr_number,
        "branch": fix_branch, "merged": _merged,
        "fixed_count": len(fixed_results),
        "total_count": len(results),
        "results": results,
    })


def _github_code_search_paths(owner: str, repo: str, token: str, terms: list[str], *, limit: int = 8) -> list[str]:
    """Find files that literally reference the given terms (e.g. image src basenames) via
    GitHub code search. Deterministic — far more reliable than guessing files from names."""
    paths: list[str] = []
    seen: set[str] = set()
    done_terms: set[str] = set()
    for term in (terms or []):
        base = str(term).rsplit("/", 1)[-1].strip()
        if not base or len(base) < 3 or base in done_terms:
            continue
        done_terms.add(base)
        if len(done_terms) > 8:
            break
        try:
            data = _github_api_get(
                "/search/code", token=token,
                params={"q": f'"{base}" repo:{owner}/{repo}', "per_page": 5}, timeout_s=15,
            )
        except Exception:
            continue
        for it in (data.get("items") or []) if isinstance(data, dict) else []:
            p = it.get("path") if isinstance(it, dict) else None
            if isinstance(p, str) and p and p not in seen and _github_file_path_allowed(p):
                seen.add(p)
                paths.append(p)
        if len(paths) >= limit:
            break
    return paths[:limit]


def _github_grep_repo_for_terms(
    owner: str, repo: str, branch: str, token: str, all_paths: list[str], terms: list[str],
    *, max_scan: int = 70, limit: int = 8,
) -> list[str]:
    """Deterministically find files whose CONTENT contains any of the terms (e.g. image src
    basenames like 'btc.svg'). Reads a bounded, source-prioritized subset of editable files.
    More reliable than GitHub code search (which tokenizes and needs an index)."""
    import base64 as _b64
    needles = _evidence_needles(terms)
    dirs: set[str] = set()
    for t in terms or []:
        ts = str(t).strip()
        if "/" in ts:
            d = ts.rsplit("/", 1)[0] + "/"  # e.g. "/images/"
            if len(d) >= 3:
                dirs.add(d.lower())
    if not needles:
        return []
    _img_tokens = ("<img", "<image", "next/image")
    cand = [
        p for p in all_paths
        if not _is_repo_noise(p)
        and ("." in p.rsplit("/", 1)[-1])
        and p.rsplit(".", 1)[-1].lower() in _EDITABLE_EXTS
    ]
    # Image refs live both in UI components and in content (markdown/MDX) — prioritize both.
    _src_dirs = (
        "components/", "app/", "src/", "pages/", "lib/", "layouts/", "templates/", "partials/",
        "content/", "posts/", "data/", "blog/", "_posts/", "articles/",
    )
    cand.sort(key=lambda p: 0 if any(d in p.lower() for d in _src_dirs) else 1)
    exact_hits: list[str] = []
    img_hits: list[str] = []
    for p in cand[:max_scan]:
        try:
            fd = _github_api_get(_github_content_api_path(owner, repo, p), token=token, params={"ref": branch}, timeout_s=12)
            raw = _b64.b64decode(fd.get("content", "").replace("\n", "")).decode("utf-8", errors="replace")
        except Exception:
            continue
        low = raw.lower()
        if any(n in raw for n in needles):
            exact_hits.append(p)  # literal src/link present
        elif any(tok in low for tok in _img_tokens) and (not dirs or any(d in low for d in dirs)):
            img_hits.append(p)  # renders an image referencing the evidence directory (dynamic src)
    # Exact literal matches first, then dynamic image renderers.
    out: list[str] = []
    for p in exact_hits + img_hits:
        if p not in out:
            out.append(p)
        if len(out) >= limit:
            break
    return out


def _evidence_needles(terms: list[str]) -> list[str]:
    """Searchable substrings for locating files that reference each term. Handles both image
    srcs (basename like 'btc.svg') and link URLs incl. trailing slash (path like
    '/mentions-legales/') — strips scheme+host so a full URL still matches a relative href."""
    out: list[str] = []
    for t in terms or []:
        ts = str(t).strip()
        if not ts:
            continue
        path = ts
        if "://" in path:
            rest = path.split("://", 1)[1]
            slash = rest.find("/")
            path = rest[slash:] if slash >= 0 else ""
        for cand in (path, ts.rsplit("/", 1)[-1]):
            cand = cand.strip()
            if len(cand) >= 3 and cand not in out:
                out.append(cand)
    return out


def _github_tarball_grep(
    owner: str, repo: str, branch: str, token: str, terms: list[str],
    *, limit: int = 8, max_bytes: int = 60_000_000, max_file: int = 600_000,
) -> list[str]:
    """Download the repo tarball ONCE and grep every editable file locally — complete and
    fast (1 request vs N). Returns paths whose content references the terms (exact substring:
    image basename or link path), or that render an image referencing the evidence dir."""
    import io
    import tarfile
    needles = _evidence_needles(terms)
    dirs: set[str] = set()
    for t in terms or []:
        ts = str(t).strip()
        if "/" in ts:
            d = ts.rsplit("/", 1)[0] + "/"
            if len(d) >= 3:
                dirs.add(d.lower())
    if not needles:
        return []
    try:
        url = _github_api_url(_github_api_path("repos", owner, repo, "tarball", *branch.split("/")))
        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json", "User-Agent": "seo-agent-web"},
            stream=True, timeout=30,
        )
        if resp.status_code != 200:
            return []
        raw = b""
        for chunk in resp.iter_content(65536):
            raw += chunk
            if len(raw) > max_bytes:
                return []
    except Exception:
        return []
    _img_tokens = ("<img", "<image", "next/image")
    exact: list[str] = []
    imgish: list[str] = []
    try:
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as tf:
            for m in tf.getmembers():
                if not m.isfile() or m.size > max_file:
                    continue
                parts = m.name.split("/", 1)  # strip leading "<repo>-<sha>/" prefix
                rel = parts[1] if len(parts) == 2 else m.name
                if _is_repo_noise(rel):
                    continue
                base = rel.rsplit("/", 1)[-1]
                if "." not in base or base.rsplit(".", 1)[-1].lower() not in _EDITABLE_EXTS:
                    continue
                if not _github_file_path_allowed(rel):
                    continue
                fobj = tf.extractfile(m)
                if fobj is None:
                    continue
                try:
                    content = fobj.read().decode("utf-8", errors="replace")
                except Exception:
                    continue
                cl = content.lower()
                if any(n in content for n in needles):
                    exact.append(rel)
                elif any(tok in cl for tok in _img_tokens) and (not dirs or any(d in cl for d in dirs)):
                    imgish.append(rel)
    except Exception:
        return []
    out: list[str] = []
    for p in exact + imgish:
        if p not in out:
            out.append(p)
        if len(out) >= limit:
            break
    return out


_LENGTH_FAMILIES: dict[str, tuple[str, ...]] = {
    "title": (
        "title_too_short", "title_too_short_indexable", "title_too_short_not_indexable",
        "title_too_long", "title_too_long_indexable", "title_too_long_not_indexable",
    ),
    "meta": (
        "meta_description_too_short", "meta_description_too_short_indexable", "meta_description_too_short_not_indexable",
        "meta_description_too_long", "meta_description_too_long_indexable", "meta_description_too_long_not_indexable",
    ),
}


def _length_family_name(issue_key: str) -> str | None:
    for name, keys in _LENGTH_FAMILIES.items():
        if issue_key in keys:
            return name
    return None


def _length_family_keys(issue_key: str) -> set[str]:
    """For a title/meta length issue, return ALL sibling keys (too-short + too-long) so one
    fix pass brings every value into the optimal window. Non-length issues return just themselves."""
    name = _length_family_name(issue_key)
    return set(_LENGTH_FAMILIES[name]) if name else {issue_key}


def _issue_evidence_srcs(issue_block: Any) -> list[str]:
    """Concrete locator strings for precise patching (e.g. src of <img> tags lacking alt),
    read from the crawler's per-issue evidence (`alt_samples`). Empty for issues without it."""
    if not isinstance(issue_block, dict):
        return []
    out: list[str] = []
    samples = issue_block.get("alt_samples")
    if isinstance(samples, dict):
        for srcs in samples.values():
            if isinstance(srcs, list):
                for s in srcs:
                    s = str(s).strip()
                    if s and s not in out:
                        out.append(s)
    return out[:30]


# Target windows for the length families, derived from the CRAWLER's own thresholds
# (seo_audit.py: TITLE_TOO_LONG=70 / TITLE_TOO_SHORT=15, DESC_TOO_LONG=160 / DESC_TOO_SHORT=100).
# They sit just under the ceiling, not far below it: the first model-written PR on a customer
# account (voiceoverstudioai.com #2) cut titles from 81 to 47-50 because the hint asked for
# 50-60 while nothing is flagged until 70 — the model obeyed, and ~20 characters of keyword
# surface were dropped on every page for no gain. The margin absorbs a template suffix
# (" | Marque") that lands on the RENDERED string. `test_length_hint_windows.py` fails if these
# ever drift outside the crawler's thresholds.
_LENGTH_WINDOWS: dict[str, tuple[int, int]] = {"title": (60, 68), "description": (140, 155)}

# The CRAWLER's own thresholds (seo_audit.py TITLE_TOO_LONG / DESC_TOO_LONG). The window is a
# preference; this is the hard bound, and the hint must state it as such. Measured on the model
# production actually uses: given only "aim for 140-155" plus "remove the least informative
# clause, do not shorten more than necessary", Claude removed ONE clause and stopped at 200
# characters — obeying the conservative half while missing the constraint the family exists for.
# A preference cannot be the only bound in a prompt whose success is a hard inequality.
_LENGTH_CEILINGS: dict[str, int] = {"title": 70, "description": 160}


def _build_length_hint(issues: dict[str, Any], family_keys: set[str], kind: str) -> str:
    """Build a corrector hint from the crawler's `length_samples` (rendered value + length per
    page), so the AI targets the optimal window on the RENDERED string (template suffix incl.)."""
    samples: dict[str, dict[str, Any]] = {}
    for k in family_keys:
        blk = issues.get(k)
        ls = blk.get("length_samples") if isinstance(blk, dict) else None
        if isinstance(ls, dict):
            for u, info in ls.items():
                if u not in samples and isinstance(info, dict):
                    samples[u] = info
    if not samples:
        return ""
    _k = _length_kind(kind)
    low, high = _LENGTH_WINDOWS[_k]
    ceiling = _LENGTH_CEILINGS[_k]
    window = f"{low}-{high} caractères"
    label = "titre" if kind == "title" else "meta description"
    lines = []
    truncated_any = False
    for u, info in list(samples.items())[:25]:
        rendered = str(info.get("rendered") or "")
        ln = info.get("len")
        # A sample shorter than the length it is labelled with is TRUNCATED. Older crawls capped
        # it at 200 characters while reporting the true length, so the model was shown a
        # 200-character string called "268 caractères" — an instruction it cannot satisfy, and it
        # answered with the string it was given. Say so rather than let it guess; the full value
        # is in the file the model already has.
        cut = isinstance(ln, int) and len(rendered) < ln
        truncated_any = truncated_any or cut
        suffix = " [EXTRAIT TRONQUÉ]" if cut else ""
        # State the number of characters to REMOVE, not just the target. A model does not count
        # characters: told only "at most 160", it removes one trailing clause and stops — 268
        # became 200 on three separate runs, above the threshold every time, whatever the wording
        # of the constraint. Subtraction is a task it can actually carry out.
        excess = f" → RETIRE AU MOINS {ln - ceiling} caractères" if isinstance(ln, int) and ln > ceiling else ""
        lines.append(f"  - {u} → {label} RENDU actuel ({ln} car.){suffix}{excess} : \"{rendered}\"")
    if truncated_any:
        lines.append(
            "  ATTENTION : les extraits marqués [EXTRAIT TRONQUÉ] sont coupés — ils ne montrent "
            "PAS la valeur entière. Lis la valeur complète dans le fichier fourni et raccourcis "
            "à partir de celle-là ; ne recopie jamais un extrait tronqué tel quel."
        )
    return (
        f"IMPORTANT — longueur sur le RENDU : ci-dessous le {label} TEL QU'IL EST RENDU (suffixe de "
        f"template inclus) et sa longueur réelle pour chaque page. Vise un RENDU de {window}. "
        f"Le rendu = ta valeur source + un éventuel suffixe de template (ex. ' | Marque') : ajuste la "
        f"source pour que le RENDU entre dans la fenêtre (ne te fie pas à la seule longueur de la source).\n"
        f"CONTRAINTE ABSOLUE : le RENDU doit faire AU PLUS {ceiling} caractères — c'est le seuil qui "
        f"déclenche l'anomalie, et rester au-dessus ne corrige RIEN. Si retirer une seule clause ne "
        f"suffit pas à passer sous {ceiling}, retires-en davantage jusqu'à y arriver. "
        f"Ensuite seulement : NE RACCOURCIS PAS PLUS QUE NÉCESSAIRE, vise le HAUT de la fenêtre ({high} car.), pas le bas. "
        f"Un {label} nettement plus court que la fenêtre perd de la surface de mots-clés sans rien "
        f"corriger. Retire la partie la MOINS informative (une clause de fin, un qualificatif redondant) "
        f"et conserve la marque, l'année et les termes de recherche déjà présents ; ne réécris pas "
        f"depuis zéro et ne traduis jamais : garde la langue de la page.\n"
        + "\n".join(lines)
    )


_REDIRECT_LINK_KEYS = {
    "page_has_links_to_redirect_indexable",
    "page_has_links_to_redirect_not_indexable",
    "page_has_links_to_redirect",
}


def _issue_redirect_pairs(issue_block: Any) -> list[dict[str, str]]:
    """Read the crawler's `redirect_link_samples` (link URL → final destination) for a
    links-to-redirect issue. Empty for other issues."""
    if not isinstance(issue_block, dict):
        return []
    samples = issue_block.get("redirect_link_samples")
    out: list[dict[str, str]] = []
    if isinstance(samples, list):
        for s in samples:
            if isinstance(s, dict) and s.get("from") and s.get("to"):
                out.append({"from": str(s["from"]), "to": str(s["to"])})
    return out[:40]


def _build_redirect_hint(pairs: list[dict[str, str]]) -> str:
    """Hint listing each link→final-destination pair to rewrite (exact replacement)."""
    if not pairs:
        return ""
    lines = [f"  - {p['from']}  →  {p['to']}" for p in pairs[:30]]
    return (
        "Liens à RÉÉCRIRE (remplace l'URL de gauche par celle de droite, EXACTEMENT, partout où "
        "elle apparaît comme lien dans ce fichier ; ne touche aucun autre lien) :\n" + "\n".join(lines)
    )


# Per-issue instructions for the hreflang / html-lang family. Each tells the AI the EXACT
# structural fix to apply in the file that generates the page's <head> hreflang/lang tags
# (Next.js `alternates.languages` / generateMetadata, an i18n helper, or a raw <link>/<html>).
_HREFLANG_HINTS: dict[str, str] = {
    "x_default_hreflang_missing": (
        "Ces pages déclarent des alternates hreflang mais PAS de x-default. Ajoute une entrée "
        "hreflang=\"x-default\" pointant vers la version par défaut (généralement la langue "
        "principale du site / la racine, souvent la même URL que l'alternate par défaut). "
        "Ne modifie AUCUN autre alternate existant."
    ),
    "html_lang_attribute_missing": (
        "La balise <html> de ces pages n'a pas d'attribut lang. Ajoute lang=\"xx\" avec le code "
        "de langue correct de la page (déduis-le de l'URL/locale ou des hreflang de la page). "
        "Ne touche à rien d'autre."
    ),
    "html_lang_attribute_invalid": (
        "L'attribut lang de <html> a une valeur invalide. Corrige-le en un code BCP-47 valide "
        "(ex. \"fr\", \"en\", \"es\", \"de\", ou \"fr-FR\"), cohérent avec la langue réelle de la page."
    ),
    "hreflang_defined_but_html_lang_missing": (
        "Ces pages ont des annotations hreflang mais <html> n'a pas d'attribut lang. Ajoute "
        "lang=\"xx\" à <html> avec le code correspondant à la langue de la page (celui de son "
        "propre hreflang auto-référencé). N'altère pas les hreflang."
    ),
    "hreflang_annotation_invalid": (
        "Certaines annotations hreflang ont un code de langue/région invalide. Corrige uniquement "
        "les codes fautifs en BCP-47 valide (langue ou langue-région, ou x-default). Ne change pas "
        "les URLs des alternates ni les codes déjà valides."
    ),
    "hreflang_to_non_canonical": (
        "Des href hreflang pointent vers une URL NON canonique (variante avec slash, .html, "
        "paramètres, ou http). Remplace chaque href hreflang par l'URL CANONIQUE correspondante "
        "(celle du <link rel=canonical> de la page cible). Ne touche qu'aux href hreflang."
    ),
    "hreflang_to_redirect_or_broken_page": (
        "Des href hreflang pointent vers une URL qui redirige (3xx) ou est cassée. Remplace chaque "
        "href hreflang par sa destination finale en 200 (jamais l'inverse). Ne touche qu'aux href "
        "hreflang, garde les codes de langue intacts."
    ),
}


# Per-issue instructions for the <head> family (canonical / Open Graph / Twitter / viewport /
# structured data). Applied in the file that generates the page head (layout/metadata/seo helper).
_HEAD_HINTS: dict[str, str] = {
    "viewport_not_set": (
        "Ces pages n'ont pas de <meta name=\"viewport\">. Ajoute exactement "
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"> dans le <head> "
        "(ou l'équivalent metadata/viewport du framework). N'ajoute rien d'autre."
    ),
    "canonical_points_to_redirect": (
        "Le <link rel=canonical> de ces pages pointe vers une URL qui REDIRIGE. Remplace la valeur "
        "du canonical par la destination FINALE en 200 (l'URL propre, sans slash/.html/paramètre "
        "superflu). Ne touche qu'au canonical."
    ),
    "non_canonical_page_specified_as_canonical_one": (
        "Le canonical de ces pages pointe vers une URL non-canonique. Fais pointer le canonical "
        "vers l'URL canonique réelle de la page (sa version indexable définitive). Ne touche qu'au canonical."
    ),
    "canonical_from_http_to_https": (
        "Ces pages sont servies en http:// alors que leur canonical pointe (correctement) vers "
        "la version https://. Le canonical est BON : ne le modifie pas. Corrige ce qui expose "
        "la page en http — un lien interne, une URL de sitemap ou une règle de redirection "
        "manquante http→https. Si rien de tel n'est dans ce fichier, ne change rien."
    ),
    "canonical_from_https_to_http": (
        "Le canonical passe de https vers http. Corrige-le en https:// (même host/chemin). Ne touche qu'au canonical."
    ),
    "open_graph_tags_missing": (
        "Ces pages n'ont pas de balises Open Graph. Ajoute les balises OG essentielles dans le "
        "<head> : og:title, og:description, og:type (website/article), og:url (= URL canonique de "
        "la page) et og:image (réutilise l'image sociale/hero existante si disponible). Reprends le "
        "titre/description déjà présents de la page."
    ),
    "open_graph_tags_incomplete": (
        "Le bloc Open Graph est incomplet. Ajoute UNIQUEMENT les balises OG manquantes parmi "
        "og:title, og:description, og:type, og:url, og:image — sans dupliquer celles déjà présentes."
    ),
    "open_graph_url_not_matching_canonical": (
        "og:url ne correspond pas au <link rel=canonical>. Aligne og:url sur l'URL canonique de "
        "CHAQUE page affectée. RÈGLE CRITIQUE (Next.js App Router) : un openGraph défini dans une "
        "page REMPLACE entièrement le openGraph hérité du layout (il n'est PAS fusionné) — donc un "
        "bare `openGraph: { url }` FAIT PERDRE og:image et casse l'OG (open_graph_tags_incomplete). "
        "Donc pour CHAQUE page affectée : si elle a déjà un openGraph, ajoute-lui juste `url`; sinon "
        "crée `openGraph: { url: '<chemin de la page>', images: <images héritées du layout> }` en "
        "REPRENANT les images OG du layout (fournies ci-dessous) pour ne pas les perdre. Une URL "
        "relative (ex. '/about') suffit si metadataBase existe. Ne MODIFIE ni ne SUPPRIME le layout ; "
        "corrige TOUTES les pages listées, pas un sous-ensemble."
    ),
    "twitter_card_missing": (
        "Ces pages n'ont pas de Twitter Card. Ajoute twitter:card (summary_large_image), "
        "twitter:title, twitter:description et twitter:image (réutilise l'image OG/sociale existante)."
    ),
    "twitter_card_incomplete": (
        "La Twitter Card est incomplète. Ajoute uniquement les balises twitter:* manquantes "
        "(twitter:card, twitter:title, twitter:description, twitter:image) sans dupliquer l'existant."
    ),
    "structured_data_schema_org_validation_error": (
        "Le JSON-LD schema.org de ces pages a une erreur de validation. Corrige UNIQUEMENT les "
        "champs invalides/manquants requis par le type de schéma déclaré (garde le type et les "
        "données existantes). Ne casse pas le JSON."
    ),
    "structured_data_google_rich_results_validation_error": (
        "Le balisage structuré a une erreur de validation Google Rich Results. Corrige les champs "
        "requis manquants/invalides du type déclaré, sans changer le type ni inventer de données."
    ),
}


# Issues whose fix must be PER-PAGE only (never touch a shared layout/template): editing a
# shared og:url/canonical there changes EVERY page (incl. already-correct ones). Deterministic
# guard — shared templates are dropped from the target set for these keys.
_PER_PAGE_ONLY_KEYS = _with_indexability_variants({
    "open_graph_url_not_matching_canonical",
    # Writing a title / description / h1 into a SHARED file gives every page the same value —
    # it converts "missing" into "duplicate" across the whole site. Before this guard, asking
    # for a meta description on 2 flagged pages targeted app/layout.tsx and pages/_document.tsx
    # and NOT the pages themselves.
    "missing_title", "missing_meta_description", "missing_h1",
    "duplicate_titles", "duplicate_meta_descriptions",
})

# Per-page content tags: the fix belongs in the flagged page's own source, so these page-target.
# `missing_canonical` is here too (the page must be reached) but deliberately NOT in the
# per-page-only set above: a shared layout that computes the canonical from the route is a
# perfectly good fix, unlike a shared literal title.
_PER_PAGE_CONTENT_KEYS = _PER_PAGE_ONLY_KEYS | _with_indexability_variants({
    "multiple_meta_description_tags", "missing_canonical", "duplicate_pages_without_canonical",
})


# "Is this file shared across many pages?" now lives in backend/repo_index.py
# (`is_shared_path`), where it also covers dynamic route templates like `app/[slug]/page.tsx`
# instead of only matching layout/_document by filename.


def _extract_layout_og_images(content: str) -> str:
    """Extract the `images: [...]` literal from a layout's `openGraph` block so a per-page
    Open Graph override can REUSE it (Next.js replaces openGraph per-segment instead of
    merging, so a bare per-page `openGraph:{url}` would drop the inherited og:image).
    Best-effort: matches a non-nested array literal. Returns '' if not found."""
    m = re.search(r"openGraph\s*:\s*\{", content or "")
    if not m:
        return ""
    window = content[m.end(): m.end() + 1200]
    m2 = re.search(r"images\s*:\s*(\[[^\]]*\])", window)
    return m2.group(1).strip() if m2 else ""


# Issues whose fix = make sure specific URLs ARE present in the sitemap output.
_SITEMAP_ADD_KEYS = {"indexable_page_not_in_sitemap"}


def _build_sitemap_hint(urls: list[str]) -> str:
    """Hint telling the AI to ADD the given indexable URLs to the sitemap this file
    produces (append entries to the existing array/return — never drop existing ones,
    never change the generation logic for other pages)."""
    clean = [str(u).strip() for u in (urls or []) if str(u).strip()][:40]
    if not clean:
        return ""
    lines = "\n".join(f"  - {u}" for u in clean)
    return (
        "Cette anomalie = des pages indexables ABSENTES du sitemap. Ajoute EXACTEMENT ces URLs "
        "au sitemap produit par ce fichier (append au tableau/à la sortie existante, en respectant "
        "le format déjà utilisé pour les autres entrées — mêmes champs lastModified/changeFrequency/"
        "priority s'ils existent). N'enlève AUCUNE entrée existante et ne modifie pas la logique de "
        "génération des autres pages :\n" + lines
    )


def _rewrite_redirect_links(content: str, pairs: list[dict[str, str]]) -> tuple[str, int]:
    """DETERMINISTIC redirect-link rewrite (no AI): replace each `from` link with its
    `to`, but ONLY where `from` appears as a COMPLETE href/link value — bounded by a
    delimiter on both sides — so a redirecting `/en/` never rewrites the valid link
    `/en/guide-etoro`. Preserves fragments (`/en/#a` → `/en#a`). Never turns a relative
    link absolute. Returns (new_content, replacements)."""
    new = content
    total = 0
    # Both writings of each pair. Reducing every pair to its PATH meant a site writing internal
    # links absolutely (`href="https://site.fr/x/"`, common in generated MDX and in content
    # pasted from the live site) matched NOTHING and the family produced an empty patch —
    # silently, because this family deliberately bypasses the AI fallback. The absolute form
    # maps to the absolute target and the path form to the path target, so a relative link is
    # never turned absolute, nor the reverse.
    variants: list[tuple[str, str]] = []
    for p in pairs or []:
        for a, b in _url_value_variants(p):
            if (a, b) not in variants:
                variants.append((a, b))
    for frm, to in variants:
        if not frm or not to or frm == to:
            continue
        # Match ONLY in real link contexts — an href attribute/prop (HTML/JSX/MDX) or a
        # markdown link `](…)` — never an arbitrary quoted string (e.g. a code literal like
        # startsWith('/en/')). `from` must also be the COMPLETE value (delimiter after), so a
        # redirecting `/en/` never rewrites the valid `/en/guide-etoro`. Fragments preserved.
        prefix = r'(href\s*[=:]\s*\{?\s*["\']|\]\()'
        pattern = re.compile(prefix + re.escape(frm) + r'(?=["\'#?)>\s])')
        new, n = pattern.subn(lambda m: m.group(1) + to, new)
        total += n
    return new, total


def _rewrite_http_to_https(content: str, hosts: list[str]) -> tuple[str, int]:
    """DETERMINISTIC mixed-content fix: rewrite `http://<host>` → `https://<host>` for the
    SITE'S OWN host(s) only (same host is guaranteed to serve https, so it's safe; external
    http hosts are left alone since they may not support https). Returns (new, count)."""
    new = content
    total = 0
    for h in hosts or []:
        h = str(h or "").strip().lower()
        if not h:
            continue
        pat = re.compile(r'http://' + re.escape(h) + r'(?=[/"\'\s>?#)\]])')
        new, n = pat.subn("https://" + h, new)
        total += n
    return new, total


def _rewrite_double_slash(content: str) -> tuple[str, int]:
    """DETERMINISTIC double-slash fix: collapse `//` → `/` inside href/src VALUES only,
    preserving the `scheme://` separator. Never touches anything outside a link value."""
    counter = {"n": 0}

    def _fix(m: "re.Match[str]") -> str:
        pre, quote, val = m.group(1), m.group(2), m.group(3)
        if "://" in val:
            scheme, rest = val.split("://", 1)
            newval = scheme + "://" + re.sub(r"/{2,}", "/", rest)
        else:
            newval = re.sub(r"/{2,}", "/", val)
        if newval != val:
            counter["n"] += 1
        return pre + quote + newval + quote

    pattern = re.compile(r'((?:href|src)\s*[=:]\s*\{?\s*)(["\'])([^"\']*)\2')
    new = pattern.sub(_fix, content)
    return new, counter["n"]


# Issue families fixed by a deterministic same-host http→https rewrite (no AI).
_MIXED_CONTENT_KEYS = {
    "https_page_has_internal_links_to_http", "https_page_links_to_http_image",
    "https_page_links_to_http_javascript", "https_page_links_to_http_css",
    "https_http_mixed_content",
}
_DOUBLE_SLASH_KEYS = {"double_slash_in_url"}

# ── Unified evidence contract ────────────────────────────────────────────────────────
# The crawler attaches per-issue evidence under a single key: {"kind": ..., "items": [...]}.
# `url_pairs` items are {page, from, to}: the value a tag currently carries and the value it
# must carry. Only the crawl knows `to` (a redirect's final destination, a target's declared
# canonical), which is exactly what lets these fixers be deterministic instead of prompted.
# Older reports predate the contract and simply have no evidence — every reader degrades to
# the previous prompt-only behaviour, so a stale report is never an error.
_URL_PAIR_KEYS = {
    "canonical_points_to_redirect",
    "non_canonical_page_specified_as_canonical_one",
    "canonical_from_https_to_http",
    "hreflang_to_redirect_or_broken_page",
    "hreflang_to_non_canonical",
}


def _issue_url_pairs(issue_block: Any) -> list[dict[str, str]]:
    """Read `evidence.items` of kind `url_pairs` from an issue block. Empty when the issue
    carries no evidence (report from before the contract, or nothing worth rewriting)."""
    if not isinstance(issue_block, dict):
        return []
    ev = issue_block.get("evidence")
    if not isinstance(ev, dict) or ev.get("kind") != "url_pairs":
        return []
    out: list[dict[str, str]] = []
    for it in ev.get("items") or []:
        if isinstance(it, dict) and it.get("from") and it.get("to"):
            out.append({
                "page": str(it.get("page") or ""),
                "from": str(it["from"]),
                "to": str(it["to"]),
            })
    return out[:40]


# Issues whose fix needs the page's CURRENT state (which tag is absent, what an invalid or
# duplicated value contains). Unlike url_pairs there is no mechanical rewrite here — the AI
# still writes the tag — but it works from the real page instead of a generic instruction.
_PAGE_VALUE_KEYS = _with_indexability_variants({
    "open_graph_tags_incomplete",
    "twitter_card_incomplete",
    "html_lang_attribute_invalid",
    "hreflang_annotation_invalid",
    "multiple_title_tags",
    "multiple_h1",
    # Duplicates: the patcher must SEE the shared value to write something different from it.
    "duplicate_titles",
    "duplicate_meta_descriptions",
})


def _open_pr_for_issue(
    *, project_id: str, issue_key: str, url: str, owner: str, repo_name: str, token: str
) -> str:
    """URL of the still-open PR already covering this issue, or '' when there is none.

    One open PR per issue is enough: a second "Créer PR" click builds a duplicate touching the
    same lines, which then conflicts with the first (that is how #8 and #9 were both opened for
    one lang fix). A merged or closed PR does NOT block — the anomaly can legitimately come back
    and deserve a fresh fix."""
    try:
        with DB.session() as db:
            task = db.scalar(select(IssueTask).where(
                IssueTask.project_id == project_id,
                IssueTask.issue_key == issue_key,
                IssueTask.url == url,
            ))
            raw_note = str(task.note or "") if task else ""
        note = json.loads(raw_note) if raw_note else {}
    except Exception:
        return ""
    if not isinstance(note, dict):
        return ""
    pr_url = str(note.get("pr_url") or "")
    try:
        pr_number = int(note.get("pr_number") or 0)
    except Exception:
        pr_number = 0
    if not pr_url or pr_number <= 0:
        return ""
    return pr_url if _github_pr_is_open(owner, repo_name, pr_number, token) else ""


# `redirect_3xx` lists every redirecting URL, and on a healthy site they are the site's OWN
# http→https / www canonicalisation — deliberate, not defects. The file that produces them
# (netlify.toml, _redirects, next.config) also carries HSTS, CSP and cache rules, so handing it
# to a patcher told to "fix 3XX redirects" is destructive. Only a URL that redirects to ITSELF
# is a config bug we can repair; everything else stays advisory.
_REDIRECT_CONFIG_KEYS = {"redirect_3xx"}
_SELF_LOOP_FIELD = "boucle: redirige vers elle-meme"


def _redirect_3xx_self_loops(issue_block: Any) -> list[str]:
    """Paths of the URLs that redirect to themselves, from the crawler's page_values evidence."""
    out: list[str] = []
    for item in _issue_page_values(issue_block):
        if item.get("field") == _SELF_LOOP_FIELD:
            path = _link_path(item.get("page", ""), keep_slash=False)
            if path and path != "/" and path not in out:
                out.append(path)
    return out


def _issue_page_values(issue_block: Any) -> list[dict[str, str]]:
    """Read `evidence.items` of kind `page_values` from an issue block."""
    if not isinstance(issue_block, dict):
        return []
    ev = issue_block.get("evidence")
    if not isinstance(ev, dict) or ev.get("kind") != "page_values":
        return []
    out: list[dict[str, str]] = []
    for it in ev.get("items") or []:
        if isinstance(it, dict) and it.get("page") and it.get("field"):
            out.append({
                "page": str(it["page"]),
                "field": str(it["field"]),
                "value": str(it.get("value") or ""),
            })
    return out[:40]


def _build_page_values_hint(items: list[dict[str, str]]) -> str:
    """Hint describing, page by page, what is actually wrong right now."""
    if not items:
        return ""
    lines = [f"  - {it['page']} → {it['field']} : {it['value']}" for it in items[:30]]
    return (
        "État RÉEL de chaque page concernée (relevé au crawl). Corrige EXACTEMENT ce qui est "
        "listé pour chaque page et ne touche à rien d'autre — surtout, n'écrase pas les valeurs "
        "déjà présentes et correctes :\n" + "\n".join(lines)
    )


def _issue_hreflang_pairs(issue_block: Any) -> list[dict[str, str]]:
    """Read `evidence.items` of kind `hreflang_pairs` — a url swap keyed on the hreflang code."""
    if not isinstance(issue_block, dict):
        return []
    ev = issue_block.get("evidence")
    if not isinstance(ev, dict) or ev.get("kind") != "hreflang_pairs":
        return []
    out: list[dict[str, str]] = []
    for it in ev.get("items") or []:
        if isinstance(it, dict) and it.get("page") and it.get("code") and it.get("from") and it.get("to"):
            out.append({
                "page": str(it["page"]), "code": str(it["code"]),
                "from": str(it["from"]), "to": str(it["to"]),
            })
    return out[:40]


def _build_url_pair_hint(pairs: list[dict[str, str]]) -> str:
    """Hint listing each current value → the value to write, for the AI fallback used when a
    file builds the URL dynamically and no literal can be rewritten."""
    if not pairs:
        return ""
    lines = [f"  - {p['from']}  →  {p['to']}" for p in pairs[:30]]
    return (
        "Valeurs EXACTES à corriger (remplace celle de gauche par celle de droite, uniquement "
        "dans la balise concernée — canonical ou hreflang — jamais dans un lien de navigation). "
        "Si l'URL est construite dynamiquement, corrige la logique qui la génère pour qu'elle "
        "produise la valeur de droite :\n" + "\n".join(lines)
    )


def _url_value_variants(pair: dict[str, str]) -> list[tuple[str, str]]:
    """Both the absolute and the path-only writing of one pair, so a file that stores the URL
    relatively keeps it relative (turning it absolute is a change nobody asked for)."""
    frm, to = str(pair.get("from") or ""), str(pair.get("to") or "")
    variants = [(frm, to)]
    frm_path, to_path = _link_path(frm), _link_path(to)
    if frm_path != frm:
        variants.append((frm_path, to_path))
    return [(a, b) for a, b in variants if a and b and a != b]


# Files that DECLARE routing rather than reference it. A URL appearing here is a rule operand,
# not a link to a page, so a value rewrite corrupts the rule instead of fixing anything.
_ROUTING_CONFIG_BASENAMES = {
    "_redirects", "_headers", "netlify.toml", "vercel.json", "wrangler.toml",
    ".htaccess", "nginx.conf", "next.config.js", "next.config.ts", "next.config.mjs",
    "middleware.ts", "middleware.js", "staticwebapp.config.json",
}


def _is_routing_config_path(path: str) -> bool:
    return (path or "").rsplit("/", 1)[-1].lower() in _ROUTING_CONFIG_BASENAMES


# Sitemap entries that point at the wrong URL. The fix is a value swap inside the sitemap, so
# it is deterministic. The REMOVE-shaped siblings (4xx/5xx/noindex/timed-out in sitemap) are not
# here: dropping an entry is a different operation and the crawler gives them no replacement.
_SITEMAP_REWRITE_KEYS = {"sitemap_3xx_redirect", "sitemap_non_canonical_page"}
# The sitemap contradicts the page's own <head> for one hreflang code. Same file, same
# restriction, but the repair edits an alternate href rather than a <loc>.
_SITEMAP_ALTERNATE_KEYS = {"more_than_one_page_for_same_language_in_hreflang"}
# Every family repaired inside the sitemap file. They must never page-target: their impacted
# URLs are the pages the sitemap TALKS ABOUT, not the file to edit.
_SITEMAP_FAMILY_KEYS = _SITEMAP_ADD_KEYS | _SITEMAP_REWRITE_KEYS | _SITEMAP_ALTERNATE_KEYS


def _is_sitemap_path(path: str) -> bool:
    """True for the file that produces the sitemap — the only place these issues can be fixed."""
    return "sitemap" in (path or "").rsplit("/", 1)[-1].lower()


_SITEMAP_LOC_RE = re.compile(r"(<loc>\s*)(.*?)(\s*</loc>)", re.I | re.S)


_SITEMAP_URL_BLOCK_RE = re.compile(r"<url\b.*?</url>", re.I | re.S)
_SITEMAP_ALT_TAG_RE = re.compile(r"<xhtml:link\b[^>]*>", re.I)
_SITEMAP_ALT_CODE_RE = re.compile(r'hreflang\s*=\s*"([^"]*)"', re.I)
_SITEMAP_ALT_HREF_RE = re.compile(r'(href\s*=\s*")([^"]*)(")', re.I)


def _rewrite_sitemap_alternates(content: str, pairs: list[dict[str, str]]) -> tuple[str, int]:
    """DETERMINISTIC fix (no AI) for a sitemap alternate that contradicts the page's own <head>.

    Scoped twice over, and both scopes are load-bearing:
      - to the `<url>` block whose `<loc>` is that page, because the same URL appears as a `<loc>`,
        as its own alternate, and as an alternate of every sibling block;
      - to the hreflang CODE, because inside one block a single URL legitimately serves several
        codes. Matching on the value alone rewrote a correct `fr` alternate while fixing
        `x-default` — caught by replaying this against the real sitemap before shipping.
    Only `xhtml:link` hrefs are touched, never a `<loc>`. Returns (new_content, replacements)."""
    by_page: dict[str, dict[tuple[str, str], str]] = {}
    for pair in pairs or []:
        page = _link_path(str(pair.get("page") or ""), keep_slash=False)
        code = str(pair.get("code") or "").strip().lower()
        frm, to = str(pair.get("from") or "").strip(), str(pair.get("to") or "").strip()
        if page and code and frm and to and frm != to:
            by_page.setdefault(page, {}).setdefault((code, frm), to)
    if not by_page:
        return content, 0
    count = 0

    def _one_block(m: "re.Match[str]") -> str:
        nonlocal count
        block = m.group(0)
        loc = re.search(r"<loc>\s*(.*?)\s*</loc>", block, re.I | re.S)
        if not loc:
            return block
        mapping = by_page.get(_link_path(loc.group(1), keep_slash=False))
        if not mapping:
            return block

        def _one_tag(t: "re.Match[str]") -> str:
            nonlocal count
            tag = t.group(0)
            code_m = _SITEMAP_ALT_CODE_RE.search(tag)
            href_m = _SITEMAP_ALT_HREF_RE.search(tag)
            if not code_m or not href_m:
                return tag
            new_href = mapping.get((code_m.group(1).strip().lower(), href_m.group(2).strip()))
            if not new_href:
                return tag
            count += 1
            return tag[: href_m.start()] + href_m.group(1) + new_href + href_m.group(3) + tag[href_m.end():]

        return _SITEMAP_ALT_TAG_RE.sub(_one_tag, block)

    return _SITEMAP_URL_BLOCK_RE.sub(_one_block, content), count


def _rewrite_sitemap_locs(content: str, pairs: list[dict[str, str]]) -> tuple[str, int]:
    """DETERMINISTIC sitemap fix (no AI): replace a flagged `<loc>` with the URL that belongs
    there — the redirect's destination, or the target's declared canonical.

    ONLY `<loc>` is touched. The `xhtml:link` hreflang alternates in the same file point at the
    same URLs and are tempting to "keep consistent", but they are a separate issue with its own
    family; widening the blast radius here is exactly how a sitemap fix once rewrote a `<loc>`
    it had no business touching. Returns (new_content, replacements)."""
    mapping: dict[str, str] = {}
    for pair in pairs or []:
        frm, to = str(pair.get("from") or "").strip(), str(pair.get("to") or "").strip()
        if frm and to and frm != to:
            mapping.setdefault(frm, to)
    if not mapping:
        return content, 0
    count = 0

    def _one(m: "re.Match[str]") -> str:
        nonlocal count
        value = m.group(2).strip()
        if value in mapping:
            count += 1
            return m.group(1) + mapping[value] + m.group(3)
        return m.group(0)

    return _SITEMAP_LOC_RE.sub(_one, content), count


# Issues whose fix = repoint an asset reference at the URL it already resolves to.
_ASSET_REWRITE_KEYS = {
    "page_has_redirected_image", "image_redirects",
    "page_has_redirected_javascript", "javascript_redirects",
    "page_has_redirected_css", "css_redirects",
}


def _rewrite_asset_srcs(content: str, pairs: list[dict[str, str]]) -> tuple[str, int]:
    """DETERMINISTIC asset-reference rewrite (no AI): replace a redirecting asset URL with its
    final destination wherever it appears as a COMPLETE value — bounded by a delimiter on both
    sides. No attribute prefix is required, so `srcset` entries past the first, CSS `url(...)`
    and JSX props are all covered; exact-value matching is what keeps it safe (unlike the link
    rewriter, an asset path can't be a prefix of a different asset path).
    Returns (new_content, replacements)."""
    new = content
    total = 0
    seen: set[str] = set()
    for pair in pairs or []:
        for frm, to in _url_value_variants(pair):
            if frm in seen:
                continue
            seen.add(frm)
            pattern = re.compile(r'(?<=[\s"\'=(,])' + re.escape(frm) + r'(?=["\'\s,)>?#])')
            new, n = pattern.subn(to, new)
            total += n
    return new, total


_LINK_TAG_RE = re.compile(r"<link\b[^>]*>", re.I)
_HREF_ATTR_RE = re.compile(r'(href\s*=\s*)(["\'])(.*?)\2', re.I | re.S)
_REL_CANONICAL_RE = re.compile(r'rel\s*=\s*["\']?(canonical|alternate)\b', re.I)
# A canonical URL is written three ways in a component: as an object property
# (`canonical: "…"`), as a binding (`const canonical = "…"`), and as a literal prop
# (`<Base canonical="…">`). Only the first was matched, so on Astro — where the <link> lives in
# the shared layout as `href={canonical}` and the VALUE sits in the page as an assignment — the
# deterministic rewriter found nothing and the family silently degraded to a model-written
# patch, losing its "mechanical fix" badge on the most idiomatic way to write a canonical.
# The lookbehind is what keeps `data-canonical=` and `mycanonical:` out; the old pattern
# rewrote `mycanonical:` too.
_JS_CANONICAL_RE = re.compile(r'(?<![\w-])(canonical\s*[:=]\s*)(["\'])(.*?)\2')
# A binding that HOLDS the alternates, in the shapes a component actually uses:
# `languages: { … }`, `alternates: [ … ]`, `const hreflangs = [ … ]`. Scoping the rewrite to
# such a block is what keeps a plain nav `href:` out of it — the rule of this rewriter is
# that a menu pointing at the same URL is deliberately left alone.
_JS_LANGUAGES_RE = re.compile(r"(?:languages|alternates|hreflangs?|alternateLinks)\s*[:=]\s*[{\[]", re.I)
_QUOTED_VALUE_RE = re.compile(r'(:\s*)(["\'])(.*?)\2')


def _bracketed_block(content: str, open_idx: int, opener: str = "{") -> tuple[int, int]:
    """Span of the `{...}` or `[...]` block starting at open_idx, bracket-matched.

    An alternates list is an ARRAY of objects, so brace matching alone stopped at the first
    inner object and saw one entry out of N. Returns (-1, -1) when unbalanced.
    """
    closer = "]" if opener == "[" else "}"
    depth = 0
    for i in range(open_idx, len(content)):
        if content[i] == opener:
            depth += 1
        elif content[i] == closer:
            depth -= 1
            if depth == 0:
                return (open_idx, i + 1)
    return (-1, -1)


def _braced_block(content: str, open_brace_idx: int) -> tuple[int, int]:
    """Span of the `{...}` block starting at open_brace_idx, brace-matched. Returns (-1, -1)
    when unbalanced (truncated/odd file) so the caller can skip it rather than guess."""
    depth = 0
    for i in range(open_brace_idx, len(content)):
        if content[i] == "{":
            depth += 1
        elif content[i] == "}":
            depth -= 1
            if depth == 0:
                return (open_brace_idx, i + 1)
    return (-1, -1)


# A page's title and description are written four ways across the stacks this product supports:
# a component prop (`title="…"`), TOML front matter (`title = "…"`), YAML front matter — quoted
# OR BARE (`title: A propos`, which every quoted-value pattern misses) — and a JS binding
# (`const title = '…'`). The lookbehind keeps `og:title`, `twitter:title` and `data-title` out:
# those are COPIES of the value, not its declaration, and rewriting a copy leaves the original
# contradicting it.
_HEAD_TEXT_FIELDS = ("title", "description")
_HEAD_TEXT_QUOTED_RE = {
    field: re.compile(
        r'(?<![\w:.-])' + field + r'\s*[:=]\s*(["\'])(?P<value>.*?)\1',
        re.I,
    )
    for field in _HEAD_TEXT_FIELDS
}
_HEAD_TEXT_BARE_YAML_RE = {
    field: re.compile(
        r'(?m)^' + field + r':[ \t]+(?P<value>(?!["\'])\S[^\r\n]*?)[ \t]*$',
        re.I,
    )
    for field in _HEAD_TEXT_FIELDS
}


# A page written as plain HTML declares its two values as MARKUP, not as a property: `<title>…`
# and `<meta name="description" content="…">`. The patterns above know a component prop, TOML,
# YAML quoted and bare, and a JS binding — every way a FRAMEWORK writes it, and none of the way
# HTML does. static-html is one of the two stacks validated in production (elevenlabs-avis.com),
# so a real customer clicking "Réécrire (PR)" was told their values were "assemblées".
# The signal is a DOCUMENT, and it has to be case-SENSITIVE on the tag: `<Head>` is Next's
# component, and matching it made the locator refuse `pages/*.js` outright — a Pages Router
# page declares its values as JS bindings above that component, and the `<title>{title}</title>`
# inside it is an expression. Measured: the case-insensitive version cost next-pages 2/2 -> 0/2.
_HTML_DOC_RE = re.compile(r"(?i:<!doctype\s+html\b)|<html[\s>]")
# No `{` or `<` inside: `<title>{title}</title>` is an expression, not a value, and rewriting it
# would replace the page's logic with one page's text.
_HTML_TITLE_RE = re.compile(r"<title\b[^>]*>(?P<value>[^<{}]*)</title>", re.I)
# The lookahead pins `name="description"` EXACTLY, so `og:description` and `twitter:description`
# — copies of the value, whose rewrite would leave the original contradicting them — stay out,
# and the attribute order stays free (`content` before `name` is just as common).
_META_DESC_RE = re.compile(
    r"<meta\b(?=[^>]*\bname\s*=\s*([\"'])description\1)[^>]*?"
    # The value stops at ITS OWN closing quote, not at any quote: with `content` written
    # before `name`, a class of "anything but < > { }" swallowed the rest of the tag and
    # returned `…ici." name="description`. Excluding both quote characters instead would
    # have broken every French description — `content="Page d'accueil"` is ordinary.
    r"\bcontent\s*=\s*(?P<q>[\"'])(?P<value>(?:(?!(?P=q))[^<>{}])*)(?P=q)[^>]*>",
    re.I,
)


def _find_head_text_in_markup(content: str, field: str) -> tuple[str, str] | None:
    """The value as HTML writes it: the `<title>` element, or the description `<meta>` tag."""
    pattern = _HTML_TITLE_RE if field == "title" else _META_DESC_RE
    match = pattern.search(content or "")
    if not match:
        return None
    value = match.group("value").strip()
    return (match.group(0), value) if value else None


def _find_head_text_value(content: str, field: str) -> tuple[str, str] | None:
    """The page's own `title` or `description` as WRITTEN in the file.

    Returns (literal, value): the exact substring to replace and the text it carries, so a
    rewrite is a bounded swap rather than a regeneration of the file. None when the value is
    assembled rather than written — a template suffix, a function call, an interpolation — in
    which case the caller must not guess at it.
    """
    key = (field or "").strip().lower()
    if key not in _HEAD_TEXT_QUOTED_RE:
        return None
    # In a full HTML document the markup is the ONLY place to look. The attribute patterns below
    # would happily match a link's `title="tooltip"` and rewrite that instead of the page title —
    # the same shape as every other loose match this project has been bitten by.
    if _HTML_DOC_RE.search(content or ""):
        return _find_head_text_in_markup(content, key)
    quoted = _HEAD_TEXT_QUOTED_RE[key].search(content or "")
    if quoted:
        return quoted.group(0), quoted.group("value")
    bare = _HEAD_TEXT_BARE_YAML_RE[key].search(content or "")
    if bare:
        return bare.group(0), bare.group("value")
    # A component or template that carries the literal markup without being a whole document.
    return _find_head_text_in_markup(content, key)


def _length_kind(family: str) -> str:
    """Normalise a length family name to the key the window/ceiling tables use.

    `_length_family_name` answers 'meta' or 'title'; `_LENGTH_WINDOWS` / `_LENGTH_CEILINGS` are
    keyed 'description' or 'title'. The hint absorbed that mismatch with an inline ternary, so
    the first consumer that looked the value up strictly got None and silently did nothing.
    """
    return "title" if str(family or "") == "title" else "description"


def _rendered_len(value: str) -> int:
    """Length of the value AS RENDERED, entities resolved.

    The crawler measures the string a browser produced, so `&amp;` counts as one character there
    and as five in the file. Measured on a real customer page (elevenlabs-avis.com): a title
    spelled 60 characters renders 56 — a whole clause of margin, on the very number this family
    exists to control. Counting the source would optimise a length nobody ever sees.
    """
    import html as _html
    return len(_html.unescape(value or ""))


def _trim_to_ceiling(value: str, ceiling: int) -> str:
    """Cut `value` to at most `ceiling` characters on a word boundary, without inventing text.

    Last resort only. Never adds an ellipsis: a meta description ending in "…" tells a reader the
    sentence was cut, which is worse than a slightly abrupt end.
    """
    v = value.strip()
    if _rendered_len(v) <= ceiling:
        return v
    # Walk back until what REMAINS fits on screen: `&amp;` is five characters in the file and
    # one in the result, so cutting at a source offset would both cut too early and risk landing
    # inside an entity.
    cut = v
    while cut and _rendered_len(cut) > ceiling:
        cut = cut[:-1]
    space = cut.rfind(" ")
    if space > len(cut) * 0.6:  # keep a whole last word unless that guts the value
        cut = cut[:space]
    cut = cut.rstrip(" ,;:-–—").rstrip()
    # AFTER the punctuation strip, not before: that strip removes a trailing `;`, which turns a
    # perfectly good `&amp;` at the end into `&amp`. Measured on `_trim_to_ceiling("abcdef &amp; ghi", 9)`.
    if "&" in cut and cut.rfind("&") > cut.rfind(";"):
        cut = cut[:cut.rfind("&")].rstrip(" ,;:-–—").rstrip()
    return cut


def _length_value_for_page(
    *, current: str, kind: str, url: str, site_name: str, model_override: str = "",
) -> str:
    """Ask the model for the VALUE alone, then make the length true ourselves — at BOTH ends.

    The family's success criterion is a numeric interval, and a model does not count characters:
    given a 268-character description and a 160 ceiling it returned 200, then 200 again under an
    explicit "AU PLUS 160", then 191 when told exactly how many characters to remove. Four runs,
    four failures — no wording fixed it, because the problem is not the wording.

    The same blindness works downwards, and it went unnoticed longer because nothing flags it:
    on a real five-page correction (PR#4) four titles landed at exactly 57 characters against a
    60-68 window. That is not an anomaly — a title is only "too short" under 15 by Ahrefs parity
    — so no crawl would ever ask for those characters back. Keyword surface simply evaporated.

    So the model does what it is good at (choosing what to keep and what to drop) and the code
    does what it is good at (measuring). One retry carries the real measurement back, whichever
    side it missed on; the best of the attempts is kept, and a deterministic trim guarantees the
    ceiling. Nothing is ever padded: too short is answered by asking for a clause the model
    itself removed, never by inventing text.
    """
    kind = _length_kind(kind)
    low, high = _LENGTH_WINDOWS[kind]
    ceiling = _LENGTH_CEILINGS[kind]
    label = "titre" if kind == "title" else "meta description"
    # These instructions are in French, and the page may not be. `_rewrite_for_query` was asked
    # the same way and answered in French on an English page four runs out of four while keeping
    # the title English — a page shipped with its two snippet lines in different languages.
    # This family shortens German, Spanish and French titles on the same account, so it runs the
    # identical risk. Naming the language is the cheap half; `_dominant_language` below is the
    # half that holds, and it abstains whenever the value does not say what it is.
    lang_before = _dominant_language(current)
    too_short = _rendered_len(current) < low
    system = (
        f"Tu es un expert SEO. On te donne le {label} ACTUEL d'une page, "
        + ("trop court. " if too_short else "trop long. ")
        + f"Réécris-le pour qu'il tienne en {low} à {high} caractères. "
        + ("Garde la langue, la marque, l'année et les termes de recherche déjà présents ; "
           "développe ce que la page traite réellement, sans jamais inventer une promesse "
           "qu'elle ne tient pas. " if too_short else
           "Garde la langue, la marque, l'année et les termes de recherche déjà présents ; "
           "retire la partie la moins informative plutôt que de réécrire depuis zéro. ")
        + (f"La page est en {_LANGUAGE_NAMES[lang_before]} : réponds dans cette langue, même si "
           "ces instructions sont dans une autre. " if lang_before in _LANGUAGE_NAMES else "")
        + 'Réponds STRICTEMENT en JSON : {"value": "..."} et rien d\'autre.'
    )
    attempt = ""
    attempts: list[str] = []
    for round_no in range(2):
        if round_no == 0:
            user = json.dumps({"url": url, "site": site_name, "actuel": current,
                               "longueur_actuelle": _rendered_len(current),
                               "cible": f"{low}-{high}"},
                              ensure_ascii=False)
        else:
            # The one thing the model cannot work out for itself: how long its own answer was.
            # Stated as an operation to carry out — remove N, or take back a clause — because
            # "aim at 60-68" is precisely the instruction it cannot follow.
            n = _rendered_len(attempt)
            problem = (f"trop long de {n - ceiling} caracteres" if n > ceiling else
                       f"trop court : {n} caracteres, il en faut {low} a {high}. Reprends un "
                       "element informatif que tu as retire (ce que la page traite vraiment), "
                       "n'invente rien" if n < low else
                       f"{n} caracteres, vise {low} a {high}")
            user = json.dumps({"url": url, "site": site_name, "actuel": current,
                               "ta_proposition": attempt, "longueur_de_ta_proposition": n,
                               "probleme": problem,
                               "cible": f"{low}-{high}"}, ensure_ascii=False)
        parsed = _correction_ai_json(system=system, user_msg=user, max_tokens=600,
                                     temperature=0.1, model_override=model_override)
        attempt = str((parsed or {}).get("value") or "").strip()
        if not attempt:
            break
        if lang_before and _dominant_language(attempt) not in ("", lang_before):
            # Translating half a page's snippet is not a rewrite, it is a defect. Refusing
            # leaves the flagged value in place, which the customer can see; a translated one is
            # a page nobody flags and everybody reads.
            logger.warning("[correction-ai] %s: reponse dans une autre langue que la page (%s), "
                           "valeur refusee", kind, lang_before)
            return ""
        attempts.append(attempt)
        if low <= _rendered_len(attempt) <= high:
            return attempt
        logger.info("[correction-ai] %s: proposition de %d car. hors fenetre %d-%d, nouvel essai",
                    kind, _rendered_len(attempt), low, high)

    if not attempts:
        return ""
    # Keep whichever attempt sits closest to the window, longer wins a tie: the point of the
    # second call was to recover surface, so a value that gave some back must not be discarded
    # for one that gave less.
    def _distance(value: str) -> int:
        n = _rendered_len(value)
        return 0 if low <= n <= high else (n - high if n > high else low - n)

    best = min(attempts, key=lambda v: (_distance(v), -_rendered_len(v)))
    if _rendered_len(best) > ceiling:
        best = _trim_to_ceiling(best, ceiling)
        logger.warning("[correction-ai] %s: le modele est reste au-dessus de %d apres 2 essais, "
                       "coupe deterministe a %d car.", kind, ceiling, _rendered_len(best))
    elif _rendered_len(best) < low:
        # Never padded: a value below the window is legal (nothing flags a title until 15) and
        # inventing words to reach a target would put a promise on the page that it does not keep.
        logger.info("[correction-ai] %s: %d car., sous la fenetre %d-%d apres 2 essais, valeur "
                    "gardee telle quelle", kind, _rendered_len(best), low, high)
    return best


# Function words that belong to ONE of the four languages the product serves. Built by taking a
# per-language list and REMOVING everything shared, because the tokens that collide ("a", "un",
# "la", "en", "des") are exactly the ones that make a naive count answer confidently and wrongly.
_LANGUAGE_MARKERS_RAW: dict[str, str] = {
    "fr": "les des une pour avec vous nos notre votre cette qui que sur dans aux du ses leur "
          "tout tous toute comment pourquoi quand est sont était ont mais ainsi plus sans chez "
          "et ne pas ou vos toutes meilleur",
    "en": "the and with for your this that what when how are was were their our from has have "
          "about into than then them they will would should could been being of to in it its "
          "is at by only every without before after best which while just",
    "de": "der die das und mit für ist sind auf aus bei nach wie wenn nicht sich auch ein eine "
          "einer eines über zum zur dass werden wurde können",
    "es": "los las una pero con del por para son como cuando también sobre entre hasta sus "
          "este esta estos estas muy más pueden hacer el al lo nuestro cada",
}


def _build_language_markers() -> dict[str, frozenset[str]]:
    seen: dict[str, int] = {}
    for words in _LANGUAGE_MARKERS_RAW.values():
        for w in set(words.split()):
            seen[w] = seen.get(w, 0) + 1
    return {
        lang: frozenset(w for w in set(words.split()) if seen[w] == 1 and len(w) > 1)
        for lang, words in _LANGUAGE_MARKERS_RAW.items()
    }


_LANGUAGE_MARKERS = _build_language_markers()
_LANGUAGE_NAMES = {"fr": "francais", "en": "anglais", "de": "allemand", "es": "espagnol"}
_WORD_RE = re.compile(r"[^\W\d_]+", re.UNICODE)


def _dominant_language(text: str) -> str:
    """'fr' | 'en' | 'de' | 'es', or '' when the text does not say clearly.

    Deliberately abstains rather than guesses: this is used to REFUSE a rewrite, and refusing a
    correct one because a five-word title carried no marker would be worse than the drift it
    guards against.
    """
    words = {w.lower() for w in _WORD_RE.findall(text or "")}
    if not words:
        return ""
    scores = sorted(
        ((len(words & markers), lang) for lang, markers in _LANGUAGE_MARKERS.items()),
        reverse=True,
    )
    best, runner_up = scores[0], scores[1]
    return best[1] if best[0] >= 2 and best[0] > runner_up[0] else ""


# Not a crawler key: no crawl ever emits it. It exists so a keyword rewrite is tracked like any
# other correction (duplicate-PR guard, PR link, corrections board) — and it is deliberately kept
# out of the post-crawl verification, which could only ever read its absence as success.
_KEYWORD_REWRITE_KEY = "keyword_snippet_rewrite"


def _rewrite_for_query(
    content: str, *, query: str, url: str, site_name: str = "", model_override: str = "",
) -> tuple[str, int]:
    """Rewrite a page's own title and description to answer a search query it already ranks for.

    This is the loop the rest of the product exists to close: Search Console says which query a
    page is seen on and not clicked, and the fix is the snippet. The page keeps its subject — it
    already ranks, so the content is right — and only the two lines a searcher actually reads
    are rewritten.

    Bounded on purpose: each value is swapped for its replacement in place, so nothing else in
    the file moves, and a value that is assembled rather than written is left alone. The length
    is enforced by `_length_value_for_page`, not requested, because a model does not count
    characters — measured four times over.
    """
    text = str(query or "").strip()
    if not text:
        return content, 0

    new_content = content
    count = 0
    for field, kind in (("title", "title"), ("description", "description")):
        found = _find_head_text_value(new_content, field)
        if not found:
            continue
        literal, current = found
        if not current.strip():
            continue
        low, high = _LENGTH_WINDOWS[kind]
        ceiling = _LENGTH_CEILINGS[kind]
        label = "titre" if kind == "title" else "meta description"
        # The page's own language, read from the value being replaced. Measured on a real
        # customer page (an English review, `lang: "en"`): asked in French to rewrite a "meta
        # description", the model returned FRENCH four times out of four while keeping the title
        # English — a page shipped with a title in one language and its description in another.
        # "Garde la langue de la page" was already in the prompt; naming the language is the
        # cheap half, and the check below is the half that holds.
        lang_before = _dominant_language(current)
        system = (
            f"Tu es un expert SEO. Une page se positionne deja sur la requete donnee mais n'est "
            f"presque jamais cliquee. Reecris son {label} pour qu'il reponde a cette requete et "
            f"donne envie de cliquer. Garde la langue de la page, sa marque et son sujet : la "
            f"page est pertinente, c'est sa presentation qui ne l'est pas. "
            + (f"La page est en {_LANGUAGE_NAMES[lang_before]} : reponds dans cette langue, "
               f"meme si la requete ou ces instructions sont dans une autre. "
               if lang_before in _LANGUAGE_NAMES else "")
            + f"Vise {low} a {high} caracteres. "
            'Reponds STRICTEMENT en JSON : {"value": "..."} et rien d\'autre.'
        )
        user = json.dumps(
            {"requete": text, "url": url, "site": site_name,
             f"{label}_actuel": current, "longueur_actuelle": _rendered_len(current),
             "langue_de_la_page": lang_before or "inconnue",
             "cible": f"{low}-{high}"},
            ensure_ascii=False,
        )
        parsed = _correction_ai_json(system=system, user_msg=user, max_tokens=600,
                                     temperature=0.2, model_override=model_override)
        proposed = str((parsed or {}).get("value") or "").strip()
        if not proposed or proposed == current:
            continue
        if lang_before and _dominant_language(proposed) not in ("", lang_before):
            # Translating a page's snippet is not a rewrite, it is a defect: half the page ends
            # up in another language than the rest. One retry, then the field is left alone —
            # keeping the old value costs the customer a click, changing the language costs them
            # the page.
            retry = _correction_ai_json(
                system=system,
                user_msg=json.dumps({**json.loads(user),
                                     "ta_proposition_refusee": proposed,
                                     "probleme": "tu as change de langue ; ecris en "
                                                 f"{_LANGUAGE_NAMES.get(lang_before, lang_before)}"},
                                    ensure_ascii=False),
                max_tokens=600, temperature=0.2, model_override=model_override)
            proposed = str((retry or {}).get("value") or "").strip()
            if not proposed or _dominant_language(proposed) not in ("", lang_before):
                logger.warning("[keywords] %s refuse : le modele a repondu dans une autre langue "
                               "que la page (%s)", field, lang_before)
                continue
        if not (low <= _rendered_len(proposed) <= high):
            # Same guarantee as the length families, on BOTH sides: PR#3's title came out at 56
            # against a 60-68 window because only the too-long case was routed here, and nothing
            # downstream ever asks for those characters back.
            proposed = _length_value_for_page(
                current=proposed, kind=kind, url=url, site_name=site_name,
                model_override=model_override,
            ) or proposed
        if not proposed or _rendered_len(proposed) > ceiling or proposed == current:
            continue
        # A quote inside the replacement would break the literal it is going into; the model is
        # not asked to escape, so a value carrying the wrong quote is refused rather than fixed.
        if ('"' in proposed and '"' in literal) or ("'" in proposed and "'" in literal.replace("\\'", "")):
            logger.info("[keywords] %s refuse : la valeur proposee contient un guillemet", field)
            continue
        new_content = new_content.replace(literal, literal.replace(current, proposed, 1), 1)
        count += 1
    return new_content, count


def _rewrite_length_values(
    content: str, samples: dict[str, dict[str, Any]], kind: str, *,
    site_name: str = "", model_override: str = "",
) -> tuple[str, int]:
    """Replace each over-long title/description with a validated shorter one, in place.

    The value is located in the file by its literal text, so nothing else moves — the family
    stops being a full-file rewrite. A rendered value that is not found verbatim (a template
    suffix, a value built from parts) yields no replacement, and the caller's AI fallback takes
    over exactly as before.
    """
    kind = _length_kind(kind)
    ceiling = _LENGTH_CEILINGS.get(kind)
    if not ceiling or not isinstance(samples, dict):
        return content, 0
    new = content
    count = 0
    for url, info in list(samples.items())[:25]:
        if not isinstance(info, dict):
            continue
        rendered = str(info.get("rendered") or "").strip()
        declared = info.get("len")
        if not rendered or not isinstance(declared, int) or declared <= ceiling:
            continue
        if len(rendered) < declared or rendered not in new:
            continue  # truncated sample, or the value is assembled rather than written
        value = _length_value_for_page(current=rendered, kind=kind, url=str(url),
                                       site_name=site_name, model_override=model_override)
        if not value or len(value) > ceiling or value == rendered:
            continue
        new = new.replace(rendered, value, 1)
        count += 1
    return new, count


def _rewrite_head_url_values(content: str, pairs: list[dict[str, str]]) -> tuple[str, int]:
    """DETERMINISTIC canonical/hreflang value rewrite (no AI).

    Only rewrites a URL that sits where a canonical or hreflang value belongs — a
    `<link rel="canonical"|"alternate">` href, a `canonical:` metadata property, or an entry
    of a Next.js `languages: {...}` map — and only when the value matches a flagged one
    EXACTLY. A navigation link pointing at the same URL is deliberately left alone: the fix is
    about which URL the page declares as canonical, not about where its menu points.
    Returns (new_content, replacements)."""
    mapping: dict[str, str] = {}
    for pair in pairs or []:
        for old, new in _url_value_variants(pair):
            mapping.setdefault(old, new)
    if not mapping:
        return content, 0
    count = 0

    def _swap_href(tag: str) -> str:
        nonlocal count
        if not _REL_CANONICAL_RE.search(tag):
            return tag

        def _one(m: "re.Match[str]") -> str:
            nonlocal count
            val = m.group(3).strip()
            if val in mapping:
                count += 1
                return m.group(1) + m.group(2) + mapping[val] + m.group(2)
            return m.group(0)

        return _HREF_ATTR_RE.sub(_one, tag)

    new = _LINK_TAG_RE.sub(lambda m: _swap_href(m.group(0)), content)

    def _one_prop(m: "re.Match[str]") -> str:
        nonlocal count
        val = m.group(3).strip()
        if val in mapping:
            count += 1
            return m.group(1) + m.group(2) + mapping[val] + m.group(2)
        return m.group(0)

    new = _JS_CANONICAL_RE.sub(_one_prop, new)

    # `languages: { 'en': '/en', ... }` — scoped to the block so a short key elsewhere
    # (`to:`, `id:`) whose value happens to match can never be rewritten.
    pos = 0
    while True:
        m = _JS_LANGUAGES_RE.search(new, pos)
        if not m:
            break
        opener = m.group(0)[-1]
        start, end = _bracketed_block(new, m.end() - 1, opener)
        if start < 0:
            pos = m.end()
            continue
        patched_block = _QUOTED_VALUE_RE.sub(_one_prop, new[start:end])
        new = new[:start] + patched_block + new[end:]
        pos = start + len(patched_block)
    return new, count


def _link_path(url: str, *, keep_slash: bool = True) -> str:
    """Reduce a URL/link to its path only (drop scheme/host/query/fragment).
    By default keeps a trailing slash so `/x/` and `/x` stay distinguishable."""
    s = str(url or "").strip()
    if not s:
        return ""
    for sep in ("#", "?"):
        if sep in s:
            s = s.split(sep, 1)[0]
    if "://" in s:
        rest = s.split("://", 1)[1]
        slash = rest.find("/")
        s = rest[slash:] if slash >= 0 else "/"
    if not s.startswith("/"):
        s = "/" + s
    if not keep_slash and len(s) > 1:
        s = s.rstrip("/")
    return s or "/"


def _classify_redirect_pairs(pairs: list[dict[str, str]]) -> tuple[list[dict[str, str]], list[str]]:
    """Split link→final-destination pairs into content-fixable vs config-loop.

    - content_pairs: the link target differs from the link (incl. trailing-slash
      like `/x/` → `/x`) → rewrite the link in the page (existing behaviour).
    - loop_paths: the link points to a URL that redirects to *itself* (identical
      path, e.g. `/sources/etoro-en` → `/sources/etoro-en`) → NOT content-fixable;
      the redirect CONFIG is broken. Returns the deduped self-loop target paths
      (root `/` excluded — that's domain-level canonicalisation, out of scope)."""
    content: list[dict[str, str]] = []
    loops: list[str] = []
    for p in pairs or []:
        frm = _link_path(p.get("from", ""))
        to = _link_path(p.get("to", ""))
        if frm and to and frm == to and frm != "/":
            if frm not in loops:
                loops.append(frm)
        else:
            content.append(p)
    return content, loops


def _locate_redirects_config(all_paths: list[str]) -> str:
    """Find the Netlify `_redirects` file in the repo tree (publish-dir first)."""
    for cand in ("public/_redirects", "_redirects", "static/_redirects", "dist/_redirects"):
        if cand in all_paths:
            return cand
    for p in all_paths:
        if p.split("/")[-1] == "_redirects":
            return p
    return ""


def _locate_flat_html_for_path(path: str, all_paths: list[str]) -> str:
    """Given a clean URL path like `/sources/etoro-en`, return the FLAT physical
    html file that serves it (e.g. `public/sources/etoro-en.html`), or '' if it is
    already a directory index / served some other way (then we don't guess)."""
    rel = path.strip("/")
    if not rel:
        return ""
    targets = {f"public/{rel}.html", f"{rel}.html", f"static/{rel}.html", f"dist/{rel}.html"}
    for p in all_paths:
        if p in targets:
            return p
    return ""


def _path_family(path: str) -> set[str]:
    """The set of `_redirects` `from` tokens that all address the same clean URL:
    `/x`, `/x/`, `/x.html`, `/x/index.html`. Used to spot self-referential rules."""
    p = "/" + path.strip("/")
    return {p, p + "/", p + ".html", p + "/index.html"}


def _strip_self_referential_rules(content: str, path: str) -> tuple[str, list[str]]:
    """Remove every `_redirects` rule whose source AND target both address the same
    clean URL family (`/x`, `/x/`, `/x.html`, `/x/index.html`) — i.e. self-referential
    canonicalisation rules. These are exactly what makes a flat `public/x.html` file
    self-redirect (Netlify already serves such a file at the clean URL with 200, no
    rules needed — proven by sibling flat files that have zero rules). A rule that
    redirects `/x` to a DIFFERENT page is left untouched."""
    fam = _path_family(path)
    removed: list[str] = []
    out: list[str] = []
    for line in content.splitlines():
        toks = line.split()
        if len(toks) >= 2 and toks[0] in fam and toks[1] in fam:
            removed.append(line.strip())
            continue
        out.append(line)
    new = "\n".join(out)
    if content.endswith("\n") and not new.endswith("\n"):
        new += "\n"
    return new, removed


def _deep_fix_redirect_config_loops(
    *, owner: str, repo_name: str, token: str, fix_branch: str,
    all_paths: list[str], loop_paths: list[str], file_state: dict[str, dict[str, str]],
) -> tuple[list[str], list[str]]:
    """Fix clean URLs that self-redirect because of conflicting `_redirects` rules.

    Root cause (Netlify static export, `trailingSlash:false`): a flat `public/x.html`
    file is ALREADY served at the clean URL `/x` with 200 by Netlify's built-in pretty
    URLs (proven by sibling flat files that carry ZERO rules). Adding the canonical
    trio (`/x.html /x 301!` + `/x /x.html 200` + `/x/ /x 301!`) makes `/x` bounce to
    itself → infinite loop. Fix = keep the flat file, REMOVE the self-referential rules.

    Idempotent & self-healing: if a previous pass wrongly converted the flat file to a
    directory index (`x/index.html` — which then loops via the trailing-slash rule on a
    no-trailing-slash site), it is restored to the flat `x.html` first. Commits into
    fix_branch. Returns (changed_paths, human notes); only acts on paths it can resolve."""
    import base64 as _b64
    changed: list[str] = []
    notes: list[str] = []

    def _read(path: str) -> tuple[str, str] | None:
        if path in file_state:
            return file_state[path]["content"], file_state[path]["sha"]
        try:
            fd = _github_api_get(_github_content_api_path(owner, repo_name, path), token=token, params={"ref": fix_branch})
            raw = _b64.b64decode(fd.get("content", "").replace("\n", "")).decode("utf-8", errors="replace")
            return raw, fd.get("sha", "")
        except Exception:
            return None

    def _put(path: str, content: str, sha: str, message: str) -> bool:
        body: dict[str, Any] = {
            "message": f"{message}\n\nGenerated by SEO Agent",
            "content": _b64.b64encode(content.encode("utf-8")).decode("ascii"),
            "branch": fix_branch,
        }
        if sha:
            body["sha"] = sha
        try:
            resp = _github_api_put(_github_content_api_path(owner, repo_name, path), token=token, json_body=body)
            file_state[path] = {"sha": str((resp.get("content") or {}).get("sha") or ""), "content": content}
            return True
        except Exception:
            return False

    def _delete(path: str, sha: str, message: str) -> None:
        try:
            _github_api_delete(
                _github_content_api_path(owner, repo_name, path), token=token,
                json_body={"message": f"{message}\n\nGenerated by SEO Agent", "sha": sha, "branch": fix_branch},
            )
            file_state.pop(path, None)
        except Exception:
            pass

    fixed_paths: list[str] = []
    for lp in loop_paths:
        rel = lp.strip("/")
        if not rel:
            continue
        flat = f"public/{rel}.html"
        dir_index = f"public/{rel}/index.html"
        existing_flat = _locate_flat_html_for_path(lp, all_paths)
        if existing_flat:
            pass  # already a flat file — good, keep it as-is
        elif dir_index in all_paths and _github_file_path_allowed(flat):
            # Self-heal a previous wrong dir-index conversion: move it back to a flat file.
            fr = _read(dir_index)
            if fr is None:
                continue
            di_content, di_sha = fr
            if not _put(flat, di_content, "", f"fix(seo): restore flat html (dir-index loops on no-slash site) — {flat}"):
                continue
            _delete(dir_index, di_sha, f"fix(seo): remove dir-index — {dir_index}")
            changed.extend([flat, dir_index])
        else:
            continue  # no source file to serve this clean URL — don't guess
        fixed_paths.append(lp)
        notes.append(f"{lp} : servi par le fichier plat (règles auto-référentielles retirées)")

    # Remove the self-referential `_redirects` rules for every fixed path.
    if fixed_paths:
        cfg_path = _locate_redirects_config(all_paths)
        if cfg_path:
            cr = _read(cfg_path)
            if cr is not None:
                cfg_content, cfg_sha = cr
                new_cfg = cfg_content
                total_removed: list[str] = []
                for lp in fixed_paths:
                    new_cfg, removed = _strip_self_referential_rules(new_cfg, lp)
                    total_removed.extend(removed)
                if total_removed and new_cfg != cfg_content and _put(cfg_path, new_cfg, cfg_sha, f"fix(seo): drop self-referential redirect rules (loop) — {cfg_path}"):
                    changed.append(cfg_path)
                    notes.append(f"{cfg_path} : {len(total_removed)} règle(s) auto-référentielle(s) retirée(s)")
    return changed, notes


def _resolve_issue_targets(
    *, all_paths: list[str], index: dict[str, Any] | None, issue_key: str, issue_label: str,
    impacted_urls: list[str], located: list[str], max_files: int,
    evidence: list[str] | None = None, wants_page_targeting: bool = False,
    ai_map: "Callable[[], list[str]] | None" = None,
    ai_pick: "Callable[[], list[str]] | None" = None,
) -> list[str]:
    """Decide WHICH repo files to patch for one issue. Pure and deterministic apart from the
    two optional AI fallbacks, so the whole ordering can be tested without network.

    Priority: evidence hits (`located`, already resolved by the caller) → per-page sources from
    the repo route map → conventional-path guesses for URLs the map missed → hardcoded candidates
    for the issue type → AI URL mapping → AI tree pick. Shared templates are then dropped for
    issues whose fix must stay per-page."""
    targets = list(located)
    # Per-page fixes (mechanical link families + head/hreflang, and the title/meta length
    # families whose value belongs in the per-page source): the flagged pages ARE the files to
    # fix, so they are PRIORITISED — the max_files cap must never patch a subset of them.
    # Two families are deliberately excluded, both for the same reason — their impacted pages
    # are not where the fix goes, so prioritising them lets the max_files cap evict the one
    # target that matters:
    #  - sitemap membership: the impacted URLs are the DATA to append; the file to edit is the
    #    sitemap generator.
    #  - asset references: a redirecting logo flags every page but its src is written once, in
    #    a shared component that the evidence locator has already found.
    want_page_targeting = (
        wants_page_targeting
        or issue_key in _HEAD_HINTS or issue_key in _HREFLANG_HINTS or issue_key in _PAGE_VALUE_KEYS
        or issue_key in _PER_PAGE_CONTENT_KEYS
        or _length_family_name(issue_key) is not None
    ) and issue_key not in _ASSET_REWRITE_KEYS and issue_key not in _SITEMAP_FAMILY_KEYS
    index_resolved_all = False
    if want_page_targeting and impacted_urls:
        priority: list[str] = []
        # The repo route map answers URL→file deterministically (framework-aware, incl. content
        # collections behind a dynamic route). Only URLs it cannot resolve fall back to guessing
        # conventional paths, which is why the guess list below stays.
        unresolved: list[str] = []
        for u in impacted_urls:
            hits = [h for h in repo_index.route_files(index or {}, u) if h in all_paths]
            if hits:
                for h in hits:
                    if h not in priority:
                        priority.append(h)
            else:
                unresolved.append(u)
        index_resolved_all = bool(index) and not unresolved and bool(priority)
        for u in unresolved:
            rel = _link_path(u, keep_slash=False).strip("/")
            if not rel:
                continue
            for cand in (
                f"public/{rel}.html", f"{rel}.html", f"public/{rel}/index.html",
                f"content/{rel}.mdx", f"content/{rel}/index.mdx",
                f"app/{rel}/page.tsx", f"src/app/{rel}/page.tsx",
                f"src/pages/{rel}.tsx", f"pages/{rel}.tsx",
            ):
                if cand in all_paths and cand not in priority:
                    priority.append(cand)
        if priority:
            # A canonical/hreflang tag lives in the head of the flagged page and NOWHERE else, so
            # once the map has named those pages they are the complete answer. The grep-located
            # files must be dropped: the evidence is a URL, and grepping for it also finds every
            # file that merely MENTIONS the page — the sitemap, the target page itself, any page
            # linking to it. That is how PR#4 rewrote sitemap.xml (including a <loc>, which would
            # have made the sitemap point at a redirecting URL) while fixing 3 hreflang tags.
            # Other page-targeting families keep their located files: for mixed-content the
            # http:// references ARE the fix and legitimately live outside the flagged pages.
            if issue_key in _URL_PAIR_KEYS and index_resolved_all:
                targets = priority
            else:
                targets = priority + [t for t in targets if t not in priority]
    # Hardcoded candidate filenames for this issue type (this is what puts app/sitemap.ts in
    # range for a sitemap issue, where the impacted pages are the DATA, not the file to edit).
    #
    # Skipped for a per-page family once the route map has resolved EVERY impacted URL: the
    # candidates are matched by BASENAME, so on a static site `index.html` matches an arbitrary
    # `de/index.html` that was never flagged. That is how PR#2 shortened a valid 160-char meta
    # description on /de (and its og:/twitter: copies) while fixing /de/blog. When the map has
    # already named the files, anything else this step adds is by definition not flagged.
    if not (want_page_targeting and index_resolved_all):
        for candidate in _seo_file_candidates_for_issue(issue_key):
            for p in all_paths:
                if (p == candidate or p.endswith(f"/{candidate}") or p.split("/")[-1] == candidate) and p not in targets:
                    targets.append(p)
                    break
    # AI mapping of impacted URLs → source files. Skipped when the repo map already resolved
    # EVERY impacted URL: the AI could only add noise there (and costs a call).
    if impacted_urls and not index_resolved_all:
        _mapper = ai_map or (lambda: _ai_map_urls_to_files(
            issue_key=issue_key, issue_label=issue_label, urls=impacted_urls,
            all_paths=all_paths, limit=max_files, evidence=evidence,
        ))
        for f in _mapper():
            if f not in targets:
                targets.append(f)
    # Last resort: let the AI pick from the tree.
    if not targets:
        _picker = ai_pick or (lambda: _ai_pick_repo_files(issue_key, issue_label, all_paths, limit=2))
        for f in _picker():
            if f not in targets:
                targets.append(f)
    # Safety: per-page issues must NEVER be fixed in a file that renders many pages.
    #  - title/meta length: a shared template (`app/[slug]/page.tsx` rendering `post.title`, or a
    #    layout with a `%s | Marque` title template) would hardcode/lengthen EVERY page of the
    #    route — that is how title_too_short 15→7 once created title_too_long 0→9.
    #  - OG url≠canonical & co: editing the shared layout changes og:url/canonical for all pages
    #    incl. the already-correct ones (it broke the home page twice before this guard existed).
    # Prompt rules were disobeyed repeatedly here, so the guard stays deterministic.
    if issue_key in _PER_PAGE_ONLY_KEYS or _length_family_name(issue_key):
        targets = [p for p in targets if not repo_index.is_shared_path(index or {}, p)]
    # An asset src is fixed where the page REFERENCES it, never in the routing config. Those
    # files mention the redirecting URL (it is the rule's source), so the evidence grep finds
    # them — and the asset rewriter requires no attribute prefix, by design, so it would rewrite
    # the rule's left-hand side and turn `/old.png -> /new.png` into a self-redirect. Only the
    # extensionless name of `_redirects` kept it out of range on elevenlabs-avis.com; netlify.toml
    # and vercel.json are editable extensions and would not have been so lucky.
    if issue_key in _ASSET_REWRITE_KEYS:
        targets = [p for p in targets if not _is_routing_config_path(p)]
    # A sitemap issue is fixed in the sitemap, full stop. The flagged URL also appears in every
    # page that links to it, so the evidence grep drags those in — the same trap that had a
    # hreflang fix rewriting sitemap.xml, mirrored.
    if issue_key in _SITEMAP_FAMILY_KEYS:
        targets = [p for p in targets if _is_sitemap_path(p)]
    return targets[:max_files]


# Families whose DIFF is mechanical but whose PREMISE is a judgement call. When an issue is a
# contradiction between two sources, repairing it means deciding which source wins — and that
# decision belongs to the site owner, not to us. A predictable diff is not the same thing as an
# uncontroversial one, and these must never auto-merge however deterministic the rewrite is.
_FIX_PREMISE_NOTES: dict[str, str] = {
    "more_than_one_page_for_same_language_in_hreflang": (
        "Ce correctif aligne le **sitemap** sur ce que déclarent tes **pages**, en tenant la page "
        "pour la source d'autorité (c'est elle que Google lit à chaque passage). Si c'est ton "
        "sitemap qui porte la convention voulue, il faut faire l'inverse — corriger les pages — et "
        "cette PR va dans le mauvais sens."
    ),
}


def _fix_premise_note(issue_key: str) -> str:
    """The assumption a fix rests on, when it rests on one. Empty for the rest."""
    for key in (issue_key, issue_key.removesuffix("_indexable").removesuffix("_not_indexable")):
        if key in _FIX_PREMISE_NOTES:
            return _FIX_PREMISE_NOTES[key]
    return ""


def _fix_nature_note(ai_written: bool, issue_key: str = "") -> str:
    """One line in the PR body telling the reviewer WHAT to check.

    Three cases, because two were not enough: a bounded rewrite fed by crawl values is
    predictable; a value the model WROTE is an editorial proposal; and a rewrite that resolves a
    contradiction between two sources is predictable in its diff but rests on a choice of which
    source wins. All three used to produce identical-looking PRs."""
    premise = _fix_premise_note(issue_key)
    if premise:
        return (
            "\n\n> ⚖️ **Hypothèse à valider avant de merger.** Le diff est mécanique et prévisible, "
            "mais il tranche une question de fond. " + premise
        )
    if ai_written:
        return (
            "\n\n> ⚠️ **À relire avant de merger.** Le contenu de ce correctif a été **rédigé par le "
            "modèle** (texte de balise, formulation). Le périmètre des fichiers est borné par le crawl, "
            "mais la valeur écrite reste une proposition éditoriale."
        )
    return (
        "\n\n> ✅ **Correctif mécanique.** Les valeurs viennent du crawl et sont appliquées par une "
        "réécriture bornée, sans modèle : le diff est prévisible."
    )


def _prepare_issue_fix(
    *, issue_key: str, issues: dict[str, Any] | None, impacted: list[str], all_paths: list[str],
    site_name: str, owner: str, repo_name: str, branch: str, token: str,
    model_override: str = "",
) -> dict[str, Any]:
    """Everything needed to fix ONE issue: locator evidence, the patch hint, the deterministic
    rewriter when the family has one, self-redirect paths for the config fixer, and a refusal
    message when the issue must not be touched at all.

    Shared by both entry points on purpose. The per-issue "Créer PR" button used to carry all of
    this inline while "Tout corriger en 1 PR" carried none of it: the bulk path had no
    deterministic rewriter, no head/hreflang/sitemap hint, no evidence, and — worst — no
    redirect-config refusal, so it would hand netlify.toml (HSTS, CSP) to a free-form patch."""
    issues = issues if isinstance(issues, dict) else {}
    block = issues.get(issue_key)
    out: dict[str, Any] = {
        "evidence": _issue_evidence_srcs(block) if issues else [],
        "extra_hint": "",
        "link_rewriter": None,
        "rewriter_ai_fallback": False,
        # A bounded rewriter is not necessarily a model-free one. See `_deep_patch_issue_files`.
        "rewriter_is_ai": False,
        "loop_paths": [],
        "refusal": None,
    }

    # Redirect config: only a URL redirecting to ITSELF is repairable. Everything else is the
    # site's deliberate canonicalisation and must not reach a patcher.
    if issue_key in _REDIRECT_CONFIG_KEYS:
        loops = _redirect_3xx_self_loops(block)
        if not loops:
            out["refusal"] = (
                "Ces redirections sont la canonicalisation volontaire du site (http→https, www→apex, "
                "suppression du .html) : ce ne sont pas des défauts et il n'y a rien à corriger dans le "
                "code. Une correction automatique toucherait le fichier qui porte aussi tes en-têtes de "
                "sécurité et de cache. Seule une URL qui se redirige vers elle-même serait réparable ici."
            )
            return out
        out["loop_paths"] = loops
        return out

    fam = _length_family_name(issue_key)
    if fam and issues:
        out["extra_hint"] = _build_length_hint(issues, _length_family_keys(issue_key), fam)
        # Value-first: the model proposes the text, the code enforces the length. Registered as
        # a `link_rewriter` like the deterministic families, with the AI fallback left ON so a
        # value that cannot be located verbatim still gets the old full-file patch.
        _len_samples: dict[str, Any] = {}
        for _k in _length_family_keys(issue_key):
            _blk = issues.get(_k)
            _ls = _blk.get("length_samples") if isinstance(_blk, dict) else None
            if isinstance(_ls, dict):
                for _u, _i in _ls.items():
                    _len_samples.setdefault(_u, _i)
        if _len_samples:
            out["link_rewriter"] = (  # noqa: E731
                lambda raw, _s=_len_samples, _f=fam, _n=site_name, _m=model_override:
                _rewrite_length_values(raw, _s, _f, site_name=_n, model_override=_m)
            )
            out["rewriter_ai_fallback"] = True
            # …but the value it writes comes from the MODEL. Without this the family shipped a
            # PR badged "correctif mécanique", billed nothing, and — on Full Access — merged
            # itself: three decisions all reading "bounded" as "no model was involved".
            out["rewriter_is_ai"] = True

    content_pairs: list[dict[str, str]] = []
    if issue_key in _REDIRECT_LINK_KEYS and issues:
        pairs = _issue_redirect_pairs(block)
        if pairs:
            content_pairs, out["loop_paths"] = _classify_redirect_pairs(pairs)
            out["evidence"] = [p["from"] for p in content_pairs]
            out["extra_hint"] = _build_redirect_hint(content_pairs) if content_pairs else ""
    if issue_key in _SITEMAP_ADD_KEYS and impacted:
        out["extra_hint"] = _build_sitemap_hint(impacted)
    if issue_key in _HREFLANG_HINTS:
        out["extra_hint"] = _HREFLANG_HINTS[issue_key]
    if issue_key in _HEAD_HINTS:
        out["extra_hint"] = _HEAD_HINTS[issue_key]
    # OG url≠canonical: reuse the layout's inherited og:image, since Next replaces openGraph
    # per-segment and a bare per-page {url} would drop it.
    if issue_key == "open_graph_url_not_matching_canonical":
        import base64 as _b64og
        for layout in ("app/layout.tsx", "src/app/layout.tsx", "app/layout.jsx", "src/app/layout.jsx"):
            if layout not in all_paths:
                continue
            try:
                fd = _github_api_get(_github_content_api_path(owner, repo_name, layout), token=token, params={"ref": branch})
                raw = _b64og.b64decode(fd.get("content", "").replace("\n", "")).decode("utf-8", errors="replace")
                og_imgs = _extract_layout_og_images(raw)
            except Exception:
                og_imgs = ""
            if og_imgs:
                out["extra_hint"] += "\nImages OG héritées du layout à RÉUTILISER dans le openGraph de chaque page (copie ce `images:` tel quel) : images: " + og_imgs
                break

    url_pairs: list[dict[str, str]] = []
    if issues and issue_key in _SITEMAP_ALTERNATE_KEYS:
        url_pairs = _issue_hreflang_pairs(block)
    elif issues and (issue_key in _URL_PAIR_KEYS or issue_key in _ASSET_REWRITE_KEYS
                     or issue_key in _SITEMAP_REWRITE_KEYS):
        url_pairs = _issue_url_pairs(block)
        if url_pairs:
            hint = _build_url_pair_hint(url_pairs)
            out["extra_hint"] = (out["extra_hint"] + "\n" + hint) if out["extra_hint"] else hint
            out["evidence"] = [p["from"] for p in url_pairs]
    if issues and issue_key in _PAGE_VALUE_KEYS:
        values = _issue_page_values(block)
        if values:
            hint = _build_page_values_hint(values)
            out["extra_hint"] = (out["extra_hint"] + "\n" + hint) if out["extra_hint"] else hint

    # ── Deterministic rewriter for the mechanical families (no AI) ──
    if url_pairs and issue_key in _SITEMAP_ALTERNATE_KEYS:
        out["link_rewriter"] = lambda raw, _p=url_pairs: _rewrite_sitemap_alternates(raw, _p)  # noqa: E731
    elif url_pairs and issue_key in _SITEMAP_REWRITE_KEYS:
        # Targets are restricted to the sitemap file, so an AI fallback can only ever see the
        # right file — useful when the sitemap is GENERATED and holds no literal <loc>.
        out["link_rewriter"] = lambda raw, _p=url_pairs: _rewrite_sitemap_locs(raw, _p)  # noqa: E731
        out["rewriter_ai_fallback"] = True
    elif url_pairs and issue_key in _ASSET_REWRITE_KEYS:
        # An unmatched src is a bundled asset: the redirect lives in the CDN, not the page.
        out["link_rewriter"] = lambda raw, _p=url_pairs: _rewrite_asset_srcs(raw, _p)  # noqa: E731
    elif url_pairs:
        out["link_rewriter"] = lambda raw, _p=url_pairs: _rewrite_head_url_values(raw, _p)  # noqa: E731
        out["rewriter_ai_fallback"] = True
    elif content_pairs:
        out["link_rewriter"] = lambda raw, _p=content_pairs: _rewrite_redirect_links(raw, _p)  # noqa: E731
    elif issue_key in _MIXED_CONTENT_KEYS:
        host = re.sub(r"^https?://", "", str(site_name or "")).strip("/").split("/")[0].lower()
        hosts = [h for h in {host, "www." + host, host[4:] if host.startswith("www.") else host} if h]
        out["link_rewriter"] = lambda raw, _h=hosts: _rewrite_http_to_https(raw, _h)  # noqa: E731
        if host:
            out["evidence"] = [f"http://{h}" for h in hosts]
    elif issue_key in _DOUBLE_SLASH_KEYS:
        out["link_rewriter"] = _rewrite_double_slash
    return out


def _deep_patch_issue_files(
    *, owner: str, repo_name: str, branch: str, token: str, fix_branch: str,
    all_paths: list[str], issue_key: str, issue_label: str, impacted_urls: list[str],
    site_name: str, file_state: dict[str, dict[str, str]], max_files: int = 8,
    evidence: list[str] | None = None, extra_hint: str = "", model_override: str = "",
    link_rewriter: "Callable[[str], tuple[str, int]] | None" = None,
    rewriter_ai_fallback: bool = False,
    rewriter_is_ai: bool = False,
    targets_override: list[str] | None = None,
    index: dict[str, Any] | None = None,
) -> tuple[list[str], list[str], list[str], bool]:
    """Resolve the source files for one issue and commit patches into fix_branch.

    Targets = repo route map ∪ hardcoded candidates ∪ AI URL→file mapping (∪ AI tree pick as
    last resort). `index` is the deterministic repo map (see backend/repo_index.py): when it
    resolves an impacted URL, that beats every heuristic below and no AI guess is needed.
    Each file is patched to fix ALL its in-file occurrences. file_state caches sha/content
    so a file edited for several issues stacks correctly across calls. When link_rewriter is
    given (mechanical link families: links-to-redirect, mixed-content http→https, double-slash),
    the per-file fix is that bounded function instead of a full-file patch.
    `rewriter_is_ai` says whether that bounded function CALLS the model anyway — the length
    families ask it for the value and enforce the length in code, and so does the keyword
    snippet rewrite. Bounded and model-free are not the same property, and three decisions read
    the difference: billing, the PR body's "who wrote this" note, and auto-merge eligibility.
    `targets_override` skips target resolution entirely for a caller that already knows the file
    (the keyword rewrite knows its page from Search Console), so no AI file-picker gets to
    choose where an editorial rewrite lands. Returns
    (patched_files, skipped_files, targets, ai_files) — `ai_files` lists the committed files the
    MODEL wrote, as opposed to those a deterministic rewrite produced. Callers use it twice: one
    entry is enough to require a human before merging, and it is what billing must count, since a
    rewrite that spends no tokens must cost the customer nothing."""
    import base64 as _b64
    targets: list[str] = []
    # 1) Deterministic: files that reference the evidence (e.g. image srcs). Tarball grep is
    #    complete (scans every file in 1 download); code search / per-file grep are fallbacks.
    if evidence:
        located: list[str] = []
        try:
            located = _github_tarball_grep(owner, repo_name, branch, token, evidence, limit=max_files)
        except Exception:
            located = []
        if not located:
            try:
                located = _github_code_search_paths(owner, repo_name, token, evidence, limit=max_files)
            except Exception:
                located = []
        if not located:
            try:
                located = _github_grep_repo_for_terms(owner, repo_name, branch, token, all_paths, evidence, limit=max_files)
            except Exception:
                located = []
        for f in located:
            if f not in targets:
                targets.append(f)
    if targets_override is not None:
        # The caller named the files. Used when the page is known independently of any crawl
        # issue (Search Console gives the URL, the repo route map gives its source), where the
        # resolution chain below could only add guesses — and its last two steps are AI file
        # pickers, the step behind every wrong-file patch this corrector has ever shipped.
        targets = [p for p in targets_override if p in all_paths][:max_files]
    else:
        targets = _resolve_issue_targets(
            all_paths=all_paths, index=index, issue_key=issue_key, issue_label=issue_label,
            impacted_urls=impacted_urls, located=targets, max_files=max_files, evidence=evidence,
            wants_page_targeting=link_rewriter is not None,
        )
    occ_hint = f"{len(impacted_urls)} page(s) du site sont touchées par cette anomalie." if impacted_urls else ""
    _idiom = repo_index.stack_idiom_hint(index) if index else ""
    if _idiom:
        occ_hint += " " + _idiom
    if evidence:
        occ_hint += " Éléments précis à corriger dans ce fichier s'ils y figurent (ex. src d'images sans alt) : " + ", ".join(evidence[:15]) + "."
    if extra_hint:
        occ_hint += " " + extra_hint
    primary_url = impacted_urls[0] if impacted_urls else ""
    patched: list[str] = []
    skipped: list[str] = []
    ai_files: list[str] = []   # the subset the MODEL wrote — the only ones that cost tokens

    def _prepare(path: str) -> tuple[str, str | None, str, dict[str, Any] | None]:
        """Read a file + generate its patch. Parallel-safe (no shared mutable state)."""
        if path in file_state:
            raw = file_state[path]["content"]
            cur_sha = file_state[path]["sha"]
        else:
            try:
                fd = _github_api_get(_github_content_api_path(owner, repo_name, path), token=token, params={"ref": fix_branch})
                raw = _b64.b64decode(fd.get("content", "").replace("\n", "")).decode("utf-8", errors="replace")
                cur_sha = fd.get("sha", "")
            except Exception:
                return (path, None, "", None)
        if len(raw) > 80_000:
            return (path, None, "", None)
        # Mechanical link families: deterministic rewrite, no AI (avoids the prefix-link and
        # relative→absolute mistakes an LLM makes; e.g. /en/ vs /en/guide, code literals).
        if link_rewriter is not None:
            new_content, n = link_rewriter(raw)
            if n > 0:
                # Marked so the caller can tell a rewrite that spent no tokens from one that did:
                # only the former is free to the customer, badgeable as mechanical, and safe to
                # merge without a human reading it. A bounded rewriter can still call the model
                # (the length families, the keyword snippet), and claiming otherwise would have
                # auto-merged model-written prose into a customer's site for free.
                return (path, raw, cur_sha,
                        {"patched_content": new_content, "deterministic": not rewriter_is_ai})
            # Nothing literal to swap. For link families that means the file simply doesn't
            # contain the link. For canonical/hreflang the URL may be BUILT (getSiteUrl(path),
            # a template literal), and only the AI can fix the logic that produces it — the
            # exact values to reach are in the hint.
            if not rewriter_ai_fallback:
                return (path, raw, cur_sha, {"no_change": True, "patched_content": raw})
        try:
            patch = _openai_generate_file_patch(
                file_path=path, file_content=raw, issue_key=issue_key, issue_label=issue_label,
                url=primary_url, site_name=site_name, occurrences_hint=occ_hint, model_override=model_override,
            )
        except Exception:
            patch = None
        return (path, raw, cur_sha, patch)

    # Generate patches concurrently (the slow AI calls dominate); preserve target order.
    if targets:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=min(5, len(targets))) as _ex:
            prepared = list(_ex.map(_prepare, targets))
    else:
        prepared = []

    # Commit sequentially — the GitHub contents API needs the current blob sha per file.
    for path, raw, cur_sha, patch in prepared:
        if raw is None or not patch or patch.get("error") or not patch.get("patched_content"):
            skipped.append(path)
            continue
        new_content = str(patch["patched_content"])
        if patch.get("no_change") or new_content.strip() == raw.strip():
            continue
        if _github_patched_content_error(new_content):
            skipped.append(path)
            continue
        try:
            put_body: dict[str, Any] = {
                "message": f"fix(seo): {issue_key} — {path}\n\nGenerated by SEO Agent",
                "content": _b64.b64encode(new_content.encode("utf-8")).decode("ascii"),
                "branch": fix_branch,
            }
            if cur_sha:
                put_body["sha"] = cur_sha
            put_resp = _github_api_put(_github_content_api_path(owner, repo_name, path), token=token, json_body=put_body)
            new_sha = str((put_resp.get("content") or {}).get("sha") or "")
            file_state[path] = {"sha": new_sha, "content": new_content}
            patched.append(path)
            if not patch.get("deterministic"):
                ai_files.append(path)
        except Exception:
            skipped.append(path)
    return patched, skipped, targets, ai_files


class _DeepFixBody(BaseModel):
    url: str = ""
    crawl_ts: str = ""


@app.post("/api/projects/{slug}/issues/{issue_key}/deep-fix")
def api_issue_deep_fix(request: Request, slug: str, issue_key: str, body: _DeepFixBody) -> JSONResponse:
    """Fix ALL occurrences of a single issue across the repo in one PR.

    Maps the issue's impacted URLs to the source file(s) (shared template/config when it
    covers everything, otherwise the per-page files), patches each, and opens one PR.
    """
    proj = _db_project_or_404(request, slug)
    user = getattr(request.state, "user", None)
    cfg = _project_github_cfg(proj)
    if not cfg["repo"]:
        return JSONResponse({"ok": False, "needs_setup": True, "error": "Aucun dépôt GitHub connecté à ce projet."}, status_code=400)
    token, source = _effective_user_connection_value(user_id=str(user.id), key="GITHUB_TOKEN")
    if not token or source != "user":
        return JSONResponse({"ok": False, "error": "GitHub non connecté."}, status_code=400)
    retry_after = _rate_limit_retry_after(bucket="github_fix_user", subject=str(getattr(user, "id", "")), limit=20, window_s=60 * 60)
    if isinstance(retry_after, int):
        return JSONResponse({"ok": False, "error": f"Trop de requêtes. Réessaie dans {_format_retry_after(retry_after)}."}, status_code=429, headers={"Retry-After": str(retry_after)})
    gate_ok, gate_msg, gate_max_files, gate_model = _correction_gate(user)
    if not gate_ok:
        return JSONResponse({"ok": False, "error": gate_msg, "billing_url": "/billing"}, status_code=402)
    repo_parts = _github_repo_parts(cfg["repo"])
    if repo_parts is None:
        return JSONResponse({"ok": False, "needs_setup": True, "error": "Configuration GitHub invalide."}, status_code=400)
    owner, repo_name = repo_parts
    branch, mode = cfg["branch"], cfg["mode"]
    if not _github_branch_allowed(branch):
        return JSONResponse({"ok": False, "error": "Branche GitHub invalide."}, status_code=400)
    meta = dash.issue_meta(issue_key)
    issue_label = meta.label if meta else issue_key
    site_name = str(proj.site_name or slug)

    # ── Impacted URLs from the crawl report ──
    runs_dir = _runs_dir_for_request(request)
    ts = (body.crawl_ts or "").strip()
    if not ts:
        crawls = dash.list_project_crawls(runs_dir, slug)
        ts = next((t for t in reversed(crawls) if dash.load_report_json(runs_dir, slug, t)), "")
    report = dash.load_report_json(runs_dir, slug, ts) if ts else None
    issues = report.get("issues") if isinstance(report, dict) and isinstance(report.get("issues"), dict) else {}
    # Length issues (title/meta) are two faces of one problem: fix too-short AND too-long
    # together so a single pass brings every value into the optimal window (no whack-a-mole).
    family_keys = _length_family_keys(issue_key)
    _impacted_set: set[str] = set()
    if issues:
        for _k in family_keys:
            if _k in issues:
                _impacted_set |= dash.extract_impacted_pages(_k, issues.get(_k))
    impacted = sorted(_impacted_set)
    if len(family_keys) > 1:
        issue_label = {"title": "Longueur des balises title", "meta": "Longueur des meta descriptions"}.get(
            _length_family_name(issue_key), issue_label
        )
    primary_url = (body.url or "").strip() or (impacted[0] if impacted else "")

    # Refuse to open a second PR while one is still awaiting review for this exact issue.
    # Checked before any branch/tree work, so a duplicate click costs nothing.
    _open_pr = _open_pr_for_issue(
        project_id=str(proj.id), issue_key=issue_key, url=primary_url,
        owner=owner, repo_name=repo_name, token=token,
    )
    if _open_pr:
        return JSONResponse({"ok": False, "duplicate": True, "pr_url": _open_pr, "error": (
            "Une PR est déjà ouverte pour cette anomalie et attend ta revue. En créer une seconde "
            "produirait le même correctif sur les mêmes lignes, donc un conflit. Merge ou ferme "
            "celle-ci d'abord."
        )}, status_code=409)

    # ── Read the repo tree once, resolve target files ──
    try:
        tree_data = _github_api_get(_github_api_path("repos", owner, repo_name, "git", "trees", branch), token=token, params={"recursive": "1"}, timeout_s=20)
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Lecture du dépôt impossible : {e}"}, status_code=400)
    all_paths = [
        item["path"] for item in (tree_data.get("tree") or [])
        if isinstance(item, dict) and item.get("type") == "blob" and _github_file_path_allowed(str(item.get("path") or ""))
    ]
    # Deterministic URL→file map + stack detection, built from the tree we just read (no extra
    # API call). It is an accelerator: an unresolved URL simply falls back to the old guessing.
    idx = repo_index.build_repo_index(all_paths)
    logger.info("[corrections] deep-fix %s %s", issue_key, repo_index.index_summary(idx))

    _prep = _prepare_issue_fix(
        issue_key=issue_key, issues=issues, impacted=impacted, all_paths=all_paths,
        site_name=str(proj.site_name or ""), owner=owner, repo_name=repo_name,
        branch=branch, token=token, model_override=gate_model,
    )
    if _prep["refusal"]:
        # Refused before the branch is created, so a dead-end click leaves nothing behind.
        return JSONResponse({"ok": False, "error": _prep["refusal"]}, status_code=422)

    # ── Create one branch, patch every impacted file, open one PR ──
    from datetime import datetime as _dt
    try:
        ref_data = _github_api_get(_github_ref_api_path(owner, repo_name, branch), token=token)
        base_sha = ref_data["object"]["sha"]
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Impossible de lire la branche {branch} : {e}"}, status_code=400)
    fix_branch = f"seo-fix/{_safe_github_branch_suffix(issue_key)}-{_dt.utcnow().strftime('%Y%m%d-%H%M%S')}"
    try:
        _github_api_post(_github_api_path("repos", owner, repo_name, "git", "refs"), token=token, json_body={"ref": f"refs/heads/{fix_branch}", "sha": base_sha})
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Impossible de créer la branche : {e}"}, status_code=400)

    evidence = _prep["evidence"]
    extra_hint = _prep["extra_hint"]
    _link_rewriter = _prep["link_rewriter"]
    _rewriter_ai_fallback = _prep["rewriter_ai_fallback"]
    _loop_paths = _prep["loop_paths"]
    file_state: dict[str, dict[str, str]] = {}
    if issue_key in _REDIRECT_CONFIG_KEYS:
        # Config-only family: the repair is the deterministic rule prune below. Never run the
        # content patcher here — its candidate list for this key is netlify.toml / next.config,
        # i.e. exactly the files that must not be rewritten from a prompt.
        patched_files, skipped, targets, _ai_files = [], [], [], []
    else:
        patched_files, skipped, targets, _ai_files = _deep_patch_issue_files(
            owner=owner, repo_name=repo_name, branch=branch, token=token, fix_branch=fix_branch,
            all_paths=all_paths, issue_key=issue_key, issue_label=issue_label, impacted_urls=impacted,
            site_name=site_name, file_state=file_state, max_files=gate_max_files, evidence=evidence,
            extra_hint=extra_hint, model_override=gate_model,
            link_rewriter=_link_rewriter, rewriter_ai_fallback=_rewriter_ai_fallback,
            rewriter_is_ai=bool(_prep["rewriter_is_ai"]), index=idx,
        )
    # Fix any self-redirect loops at the config level (flat .html → dir-index + _redirects prune).
    config_changes: list[str] = []
    config_notes: list[str] = []
    if _loop_paths:
        try:
            config_changes, config_notes = _deep_fix_redirect_config_loops(
                owner=owner, repo_name=repo_name, token=token, fix_branch=fix_branch,
                all_paths=all_paths, loop_paths=_loop_paths[:gate_max_files], file_state=file_state,
            )
        except Exception:
            config_changes, config_notes = [], []
    all_changed = patched_files + config_changes
    if not targets and not config_changes:
        return JSONResponse({"ok": False, "error": "Aucun fichier corrigeable trouvé pour cette anomalie dans le dépôt. Vérifie que le dépôt connecté contient le code source du site."}, status_code=422)
    if not all_changed:
        _ev = (" Éléments détectés (échantillon) : " + ", ".join(evidence[:5])) if evidence else " Aucune evidence captée (relance un crawl récent)."
        return JSONResponse({"ok": False, "error": f"Aucun fichier patché (essayés : {', '.join(targets)}).{_ev}", "skipped": skipped, "evidence": evidence[:10]}, status_code=422)

    # Config changes touch routing → always open a PR for human review (never auto-merge).
    _config_note_block = ("\n\n**Correction config (boucle de redirection) :**\n" + "\n".join(f"- {n}" for n in config_notes)) if config_notes else ""
    pr_title = f"fix(seo): {issue_label} — {len(all_changed)} fichier(s)"
    pr_body = (
        f"## Correction SEO automatique (couverture étendue)\n\n"
        f"**Anomalie :** {issue_label} (`{issue_key}`)\n"
        f"**Pages impactées :** {len(impacted) or '—'}\n"
        f"**Fichiers modifiés :** {len(all_changed)}\n\n"
        + "\n".join(f"- `{p}`" for p in all_changed)
        + _config_note_block
        + _fix_nature_note(bool(_ai_files), issue_key)
        + f"\n\nGénéré par [SEO Agent](https://noyaru.com) pour **{site_name}**."
    )
    try:
        pr_data = _github_api_post(_github_api_path("repos", owner, repo_name, "pulls"), token=token, json_body={"title": pr_title, "body": pr_body, "head": fix_branch, "base": branch})
        pr_url = pr_data.get("html_url", "")
        pr_number = pr_data.get("number", 0)
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Erreur lors de la création de la PR : {e}"}, status_code=400)

    _merged = False
    # Auto-merge only what a human doesn't need to read: routing changes are risky, and a value
    # WRITTEN by the model is an editorial proposal, not a mechanical repair.
    if mode == "auto" and pr_number and not config_changes and not _ai_files and not _fix_premise_note(issue_key):
        try:
            _github_api_put(_github_api_path("repos", owner, repo_name, "pulls", str(int(pr_number)), "merge"), token=token, json_body={"merge_method": "squash", "commit_title": pr_title})
            _merged = True
        except Exception:
            pass

    try:
        _note = json.dumps({"pr_url": pr_url, "pr_number": int(pr_number) if pr_number else 0, "branch": fix_branch, "files": all_changed, "deep": True, "pages": len(impacted), "config": bool(config_changes)}, ensure_ascii=False)
        with DB.session() as _db:
            _ex = _db.scalar(select(IssueTask).where(IssueTask.project_id == proj.id, IssueTask.issue_key == issue_key, IssueTask.url == primary_url))
            if _ex:
                _ex.status = "done" if _merged else "in_progress"
                _ex.note = _note
            else:
                _db.add(IssueTask(
                    project_id=str(proj.id), user_id=str(getattr(user, "id", "") or ""),
                    issue_key=issue_key, issue_label=issue_label, crawl_ts=ts, url=primary_url,
                    status="done" if _merged else "in_progress",
                    severity=str((meta.severity if meta else None) or "notice"), note=_note,
                ))
            _db.commit()
    except Exception:
        pass

    # Bill the corrections (1 per file patched = 1 AI call) against the monthly quota.
    # Config-loop file ops (rename + _redirects prune) are deterministic (no AI call) → not billed.
    # Bill the model-written files only. A deterministic rewrite makes no API call, so charging
    # it would sell compute that was never spent.
    _correction_charge(user, len(_ai_files))

    return JSONResponse({
        "ok": True, "pr_url": pr_url, "pr_number": pr_number, "branch": fix_branch,
        "merged": _merged, "files": all_changed, "files_count": len(all_changed),
        "config_fixed": config_notes, "pages_count": len(impacted), "skipped": skipped,
    })


_TASK_STATUSES = {"todo", "in_progress", "done", "ignored"}


class _IssueTaskBody(BaseModel):
    status: str
    url: str | None = None
    note: str | None = None
    issue_label: str | None = None
    crawl_ts: str | None = None
    severity: str | None = None


@app.post("/api/projects/{slug}/issues/{issue_key}/task")
def api_issue_task_upsert(request: Request, slug: str, issue_key: str, body: _IssueTaskBody) -> JSONResponse:
    proj_row = _db_project_or_404(request, slug)
    user = getattr(request.state, "user", None)
    status = body.status if body.status in _TASK_STATUSES else "todo"
    url = (body.url or "").strip() or None
    with DB.session() as db:
        stmt = select(IssueTask).where(
            IssueTask.project_id == proj_row.id,
            IssueTask.issue_key == issue_key,
            IssueTask.url == url,
        )
        task = db.scalar(stmt)
        if task:
            task.status = status
            if body.note is not None:
                task.note = (body.note or "").strip() or None
        else:
            task = IssueTask(
                project_id=str(proj_row.id),
                user_id=str(getattr(user, "id", "") or ""),
                issue_key=issue_key,
                issue_label=str(body.issue_label or issue_key),
                crawl_ts=str(body.crawl_ts or ""),
                url=url,
                status=status,
                note=(body.note or "").strip() or None,
                severity=str(body.severity or "notice"),
            )
            db.add(task)
        db.commit()
        return JSONResponse({"ok": True, "status": status})


@app.get("/api/projects/{slug}/tasks")
def api_project_tasks(request: Request, slug: str) -> JSONResponse:
    proj_row = _db_project_or_404(request, slug)
    with DB.session() as db:
        tasks = list(db.scalars(
            select(IssueTask)
            .where(IssueTask.project_id == proj_row.id)
            .order_by(IssueTask.updated_at.desc())
        ))
    return JSONResponse([{
        "id": t.id,
        "issue_key": t.issue_key,
        "issue_label": t.issue_label,
        "url": t.url,
        "status": t.status,
        "note": t.note,
        "severity": t.severity,
        "crawl_ts": t.crawl_ts,
        "updated_at": t.updated_at.isoformat() if t.updated_at else None,
    } for t in tasks])


@app.get("/projects/{slug}/automation", response_class=HTMLResponse)
def project_automation(request: Request, slug: str) -> HTMLResponse:
    proj_row = _db_project_or_404(request, slug)
    project_ctx = {
        "slug": proj_row.slug,
        "site_name": proj_row.site_name,
        "base_url": proj_row.base_url,
    }
    cfg = _project_github_cfg(proj_row)
    # Load IssueTask records for this project
    tasks_by_status: dict[str, list[Any]] = {"todo": [], "in_progress": [], "done": [], "ignored": []}
    counts: dict[str, int] = {"todo": 0, "in_progress": 0, "done": 0, "ignored": 0}
    try:
        with DB.session() as db:
            tasks_raw = list(db.scalars(
                select(IssueTask)
                .where(IssueTask.project_id == proj_row.id)
                .order_by(IssueTask.updated_at.desc())
            ))
        for t in tasks_raw:
            try:
                note_parsed = json.loads(t.note) if t.note else {}
            except Exception:
                note_parsed = {}
            pr_data = note_parsed if note_parsed.get("pr_url") else {}
            st = t.status if t.status in tasks_by_status else "todo"
            counts[st] = counts.get(st, 0) + 1
            tasks_by_status[st].append({
                "id": str(t.id), "issue_key": t.issue_key, "issue_label": t.issue_label,
                "url": t.url, "status": st, "pr": pr_data,
                "severity": t.severity, "crawl_ts": t.crawl_ts,
                "updated_at": t.updated_at.isoformat() if t.updated_at else "",
            })
    except Exception:
        pass
    resp = templates.TemplateResponse(
        "project_automation.html",
        {
            "request": request,
            "project": project_ctx,
            "slug": proj_row.slug,
            "github_cfg": cfg,
            "tasks_by_status": tasks_by_status,
            "counts": counts,
            "total": sum(counts.values()),
        },
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.get("/projects/{slug}/corrections", response_class=HTMLResponse)
def project_corrections(request: Request, slug: str) -> HTMLResponse:
    proj_row = _db_project_or_404(request, slug)
    project_ctx = {
        "slug": proj_row.slug,
        "site_name": proj_row.site_name,
        "base_url": proj_row.base_url,
    }
    github_cfg = _project_github_cfg(proj_row)
    groups: dict[str, list[Any]] = {"todo": [], "in_progress": [], "done": [], "ignored": []}
    task_lookup: dict[tuple[str, str], dict[str, Any]] = {}
    total = 0
    try:
        with DB.session() as db:
            tasks = list(db.scalars(
                select(IssueTask)
                .where(IssueTask.project_id == proj_row.id)
                .order_by(IssueTask.updated_at.desc())
            ))
        for t in tasks:
            s = t.status if t.status in groups else "todo"
            # Separate internal PR JSON from user-written note
            try:
                _note_parsed = json.loads(t.note) if t.note else {}
            except Exception:
                _note_parsed = {}
            _is_pr_note = bool(_note_parsed.get("pr_url"))
            pr_data = _note_parsed if _is_pr_note else {}
            # User-written note: only when the note isn't our internal JSON payload.
            user_note = "" if (_is_pr_note or isinstance(_note_parsed, dict) and _note_parsed.get("verify")) else (t.note or "")
            _verify = _note_parsed.get("verify") if isinstance(_note_parsed.get("verify"), dict) else None
            task_ctx = {
                "id": t.id, "issue_key": t.issue_key, "issue_label": t.issue_label,
                "url": t.url, "status": t.status,
                "pr": pr_data, "user_note": user_note, "verify": _verify,
                "severity": t.severity, "crawl_ts": t.crawl_ts,
                "updated_at": t.updated_at.isoformat() if t.updated_at else "",
            }
            groups[s].append(task_ctx)
            task_lookup[(str(t.issue_key or ""), str(t.url or ""))] = task_ctx
        total = len(tasks)
    except Exception as exc:
        import traceback
        print(f"[corrections] DB error for slug={slug}: {exc}\n{traceback.format_exc()}", flush=True)

    def _verify_result(ctx: dict[str, Any]) -> str:
        v = ctx.get("verify")
        return str(v.get("result")) if isinstance(v, dict) else ""

    stats = {
        "total": total,
        "todo": len(groups["todo"]),
        "in_progress": len(groups["in_progress"]),
        "done": len(groups["done"]),
        "ignored": len(groups["ignored"]),
        "verified_resolved": sum(
            1 for g in groups.values() for t in g if _verify_result(t) == "resolved"
        ),
        "still_present": sum(
            1 for g in groups.values() for t in g if _verify_result(t) == "still_present"
        ),
        "with_pr": sum(
            1 for g in groups.values() for t in g if (t.get("pr") or {}).get("pr_url")
        ),
    }

    runs_dir = _runs_dir_for_request(request)
    current_crawl_ts = ""
    fix_candidates: list[dict[str, Any]] = []
    try:
        crawls = dash.list_project_crawls(runs_dir, slug)
        current_crawl_ts = next((t for t in reversed(crawls) if dash.load_report_json(runs_dir, slug, t)), "")
        report = dash.load_report_json(runs_dir, slug, current_crawl_ts) if current_crawl_ts else None
        if isinstance(report, dict):
            fix_candidates = _github_fixable_issue_candidates(report=report, proj=proj_row, limit=8)
            user = getattr(request.state, "user", None)
            gh_token = ""
            if user is not None and github_cfg.get("repo"):
                gh_token, _src = _effective_user_connection_value(
                    user_id=str(getattr(user, "id", "") or ""), key="GITHUB_TOKEN"
                )
            repo_parts = _github_repo_parts(github_cfg.get("repo") or "")
            merged_cache: dict[int, bool] = {}
            for candidate in fix_candidates:
                linked = task_lookup.get((str(candidate.get("key") or ""), str(candidate.get("url") or "")))
                candidate["task_status"] = str(linked.get("status") or "") if linked else ""
                candidate["verify"] = linked.get("verify") if linked else None
                pr = linked.get("pr") if linked else {}
                pr_number = int(pr.get("pr_number") or 0) if isinstance(pr, dict) else 0
                # Une fois la PR mergée, on ne montre plus le lien « PR existante » :
                # l'anomalie reste candidate (le crawl la voit encore), mais le suivi
                # passe désormais par le workflow, pas par une nouvelle PR.
                if pr and pr.get("pr_url") and repo_parts and gh_token and pr_number > 0:
                    if pr_number not in merged_cache:
                        merged_cache[pr_number] = _github_pr_merged(
                            repo_parts[0], repo_parts[1], pr_number, gh_token
                        )
                    if merged_cache[pr_number]:
                        pr = {}
                candidate["pr"] = pr
    except Exception as exc:
        logger.warning("[corrections] failed to build correction candidates for %s: %s", slug, exc)

    resp = templates.TemplateResponse(
        "corrections.html",
        {
            "request": request,
            "project": project_ctx,
            "slug": slug,
            "groups": groups,
            "total": total,
            "github_cfg": github_cfg,
            "current_crawl_ts": current_crawl_ts,
            "fix_candidates": fix_candidates,
            "stats": stats,
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


@app.get("/projects/{slug}/export/issues-all-urls.csv")
def export_project_issues_all_urls_csv(
    request: Request,
    slug: str,
    crawl: str | None = None,
    severity: str | None = None,
    category: str | None = None,
    q: str | None = None,
) -> Response:
    """CSV with one row per (issue, affected URL) — all URLs, no sample limit."""
    _ = _db_project_or_404(request, slug)
    runs_dir = _runs_dir_for_request(request)
    data = dash.project_overview(runs_dir, slug, timestamp=crawl, compare_to=None)
    if not data:
        raise HTTPException(status_code=404, detail="Projet introuvable")

    cur = data.get("current") or {}
    ts = str(cur.get("timestamp") or "")
    summary = cur.get("summary") or {}
    issues = summary.get("issues") if isinstance(summary.get("issues"), list) else []
    issues_filtered = dash.filter_issues(issues, severity=severity, category=category, query=q)
    report = dash.load_report_json(runs_dir, slug, ts) if ts else None
    raw_issues: dict[str, Any] = (report.get("issues") or {}) if isinstance(report, dict) else {}

    def _extract_all_url_rows(issue_key: str, block: Any) -> list[dict[str, str]]:
        """Return list of {url, detail} dicts for every affected URL in the block."""
        out: list[dict[str, str]] = []
        if not isinstance(block, dict):
            return out
        examples = block.get("examples")
        if not isinstance(examples, list):
            return out
        seen: set[str] = set()
        for ex in examples:
            url = detail = ""
            if isinstance(ex, str):
                if "->" in ex:
                    parts = ex.split("->", 1)
                    url = parts[0].strip()
                    detail = parts[1].strip()
                elif " — " in ex:
                    parts = ex.split(" — ", 1)
                    url = parts[0].strip()
                    detail = parts[1].strip()
                else:
                    url = ex.strip()
            elif isinstance(ex, dict):
                url = str(ex.get("source_url") or ex.get("source") or ex.get("url") or "")
                tgt = ex.get("target_url") or ex.get("target") or ex.get("href") or ""
                anchor = ex.get("anchor") or ex.get("text") or ""
                extra_parts = []
                if tgt:
                    extra_parts.append(f"→ {tgt}")
                if anchor:
                    extra_parts.append(f'ancre: "{anchor}"')
                for k in ("title", "meta_description", "canonical", "status_code", "value"):
                    v = ex.get(k)
                    if v is not None and str(v):
                        extra_parts.append(f"{k}: {v}")
                detail = " | ".join(extra_parts)
            if url and url not in seen:
                seen.add(url)
                out.append({"url": url, "detail": detail})
        return out

    rows: list[dict[str, Any]] = []
    site_name = str(data.get("site_name") or slug)
    base_url = str(data.get("base_url") or "")

    for it in issues_filtered:
        issue_key = str(it.get("key") or "")
        block = raw_issues.get(issue_key)
        url_rows = _extract_all_url_rows(issue_key, block)
        if not url_rows:
            rows.append({
                "timestamp": ts,
                "site_name": site_name,
                "base_url": base_url,
                "severity": str(it.get("severity") or ""),
                "category": str(it.get("category") or ""),
                "issue_key": issue_key,
                "issue_label": str(it.get("label") or ""),
                "count": int(it.get("count") or 0),
                "url": "",
                "detail": "",
            })
        else:
            for ur in url_rows:
                rows.append({
                    "timestamp": ts,
                    "site_name": site_name,
                    "base_url": base_url,
                    "severity": str(it.get("severity") or ""),
                    "category": str(it.get("category") or ""),
                    "issue_key": issue_key,
                    "issue_label": str(it.get("label") or ""),
                    "count": int(it.get("count") or 0),
                    "url": ur["url"],
                    "detail": ur["detail"],
                })

    fieldnames = [
        "timestamp",
        "site_name",
        "base_url",
        "severity",
        "category",
        "issue_key",
        "issue_label",
        "count",
        "url",
        "detail",
    ]
    content = _csv_bytes(rows, fieldnames=fieldnames)
    suffix = []
    if severity:
        suffix.append(str(severity))
    if category:
        suffix.append(str(category))
    filename = f"{slug}-{ts}-issues-all-urls" + (f"-{'-'.join(suffix)}" if suffix else "") + ".csv"
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
    timing = _crawl_timing_map(slug)
    resp = templates.TemplateResponse(
        "crawls.html",
        {
            "request": request,
            "project": {"slug": slug},
            "slug": slug,
            "crawls": list(reversed(crawls)),
            "timing": timing,
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


def _crawl_items_fallback(
    runs_dir: Path, slug: str, source: str, dim: str, limit: int
) -> list[dict[str, Any]]:
    """Read per-item data from the last successful crawl CSV when the live API returns no results."""
    crawls = dash.list_project_crawls(runs_dir, slug)
    for ts in reversed(crawls):
        report = dash.load_report_json(runs_dir, slug, ts)
        if not isinstance(report, dict):
            continue
        meta = report.get("meta") if isinstance(report.get("meta"), dict) else {}
        if source == "gsc":
            src_meta = meta.get("gsc_api") if isinstance(meta.get("gsc_api"), dict) else {}
        elif source == "bing":
            src_meta = meta.get("bing") if isinstance(meta.get("bing"), dict) else {}
        else:
            return []
        if not src_meta.get("ok"):
            continue
        csv_key = "queries_csv" if dim == "query" else "pages_csv"
        csv_path_str = str(src_meta.get(csv_key) or "").strip()
        if not csv_path_str:
            continue
        csv_path = Path(csv_path_str)
        if not csv_path.exists():
            _ensure_runs_artifact_local(csv_path)
        if not csv_path.exists():
            continue
        try:
            rows = _read_gsc_csv_rows(csv_path)
        except Exception:
            continue
        if rows:
            rows.sort(key=lambda r: (-_to_int(r.get("clicks")), -_to_int(r.get("impressions"))))
            return rows[:limit]
    return []


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


@app.get("/projects/{slug}/keywords/opportunities", response_class=HTMLResponse)
def project_keyword_opportunities(request: Request, slug: str, days: int | None = None) -> HTMLResponse:
    """What to DO about the queries this site ranks for.

    The performance page reports; this one recommends. It asks Search Console for the query AND
    the page together, because a keyword opportunity that names only the query is a remark, and
    one that names the page is something the corrector can be pointed at.
    """
    proj_row = _db_project_or_404(request, slug)
    runs_dir = _runs_dir_for_request(request)
    project = dash.project_overview(runs_dir, slug, timestamp=None, compare_to=None) or {
        "slug": slug,
        "site_name": str(proj_row.site_name or slug),
        "base_url": str(proj_row.base_url or ""),
        "crawls": [],
        "current": {"timestamp": ""},
    }
    user = getattr(request.state, "user", None)
    _, gsc_cfg, _bing_cfg = _effective_project_crawl_settings(
        slug,
        config_path=DEFAULT_CONFIG if DEFAULT_CONFIG.exists() else None,
        project_settings=(proj_row.settings if isinstance(proj_row.settings, dict) else {}),
    )
    requested_days = max(1, min(int(days or int(gsc_cfg.get("days") or 28)), 365))

    payload = _fetch_gsc_live_items(
        user_id=str(getattr(user, "id", "")),
        slug=slug,
        base_url=str(proj_row.base_url or ""),
        gsc_cfg=gsc_cfg,
        days=requested_days,
        dim="query_page",
        limit=5000,
    )
    rows = payload.get("items") if isinstance(payload.get("items"), list) else []
    opportunities = keywords_mod.find_opportunities(rows)
    summary = keywords_mod.summarise(opportunities)

    with DB.session() as db:
        tracked_rows = list(db.scalars(
            select(TrackedKeyword)
            .where(TrackedKeyword.project_id == str(proj_row.id))
            .order_by(TrackedKeyword.created_at.desc())
        ))
        tracked = [
            {"id": str(r.id), "query": r.query, "target_url": r.target_url or "",
             "source": r.source, "status": r.status}
            for r in tracked_rows
        ]
    already = {t["query"] for t in tracked}

    resp = templates.TemplateResponse(
        "keyword_opportunities.html",
        {
            "request": request,
            "project": project,
            "slug": slug,
            "opportunities": opportunities,
            "summary": summary,
            "tracked": tracked,
            "already_tracked": already,
            "days": requested_days,
            # Say WHY the list is empty rather than showing an empty table: "not connected" and
            # "nothing to report" are different answers and lead to different actions.
            "gsc_ok": bool(payload.get("ok")),
            "gsc_reason": str(payload.get("reason") or ""),
            # The rewrite button opens a PR, so it only renders when a repo is connected.
            "github_cfg": _project_github_cfg(proj_row),
        },
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.post("/projects/{slug}/keywords/track")
def project_keyword_track(
    request: Request,
    slug: str,
    query: str = Form(default=""),
    target_url: str = Form(default=""),
    source: str = Form(default="gsc_opportunity"),
) -> RedirectResponse:
    proj = _db_project_or_404(request, slug)
    user = getattr(request.state, "user", None)
    if not user:
        return RedirectResponse(url="/auth/login", status_code=303)
    text = (query or "").strip()
    if not text:
        return RedirectResponse(url=f"/projects/{slug}/keywords/opportunities", status_code=303)

    try:
        with DB.session() as db:
            existing = db.scalar(
                select(TrackedKeyword).where(
                    TrackedKeyword.project_id == str(proj.id), TrackedKeyword.query == text
                )
            )
            if existing:
                # Tracking the same query twice is the same decision; refresh the target instead
                # of failing on the unique constraint.
                if target_url.strip():
                    existing.target_url = target_url.strip()
                db.commit()
            else:
                db.add(TrackedKeyword(
                    project_id=str(proj.id), user_id=str(user.id), query=text[:512],
                    target_url=(target_url.strip() or None),
                    source=(source or "manual").strip()[:32],
                ))
                db.commit()
    except Exception as e:
        logger.error("[keywords] track failed: %s: %s", type(e).__name__, e)
        return RedirectResponse(
            url=_path_with_flash(f"/projects/{slug}/keywords/opportunities",
                                 err="Impossible d'ajouter ce mot-clé pour le moment."),
            status_code=303,
        )
    return RedirectResponse(url=f"/projects/{slug}/keywords/opportunities", status_code=303)


@app.post("/projects/{slug}/keywords/untrack")
def project_keyword_untrack(request: Request, slug: str, keyword_id: str = Form(default="")) -> RedirectResponse:
    proj = _db_project_or_404(request, slug)
    if not getattr(request.state, "user", None):
        return RedirectResponse(url="/auth/login", status_code=303)
    kid = (keyword_id or "").strip()
    if kid:
        with DB.session() as db:
            row = db.get(TrackedKeyword, kid)
            # Scoped to the project from the URL, which ownership already checked: an id alone
            # must never be enough to delete another account's row.
            if row and str(row.project_id) == str(proj.id):
                db.delete(row)
                db.commit()
    return RedirectResponse(url=f"/projects/{slug}/keywords/opportunities", status_code=303)


class _KeywordRewriteBody(BaseModel):
    query: str = ""
    url: str = ""


def _same_site_url(candidate: str, base_url: str) -> bool:
    """Is `candidate` a page of the project's own site?

    The query and the page come from Search Console, but they reach this endpoint through the
    browser, and this endpoint opens a pull request on a customer's repository. A URL from
    another host names a page this repo does not own.
    """
    try:
        from urllib.parse import urlparse
        a, b = urlparse(candidate or ""), urlparse(base_url or "")
    except Exception:
        return False
    if a.scheme not in ("http", "https") or not a.netloc or not b.netloc:
        return False
    return a.netloc.lower().removeprefix("www.") == b.netloc.lower().removeprefix("www.")


@app.post("/api/projects/{slug}/keywords/rewrite-pr")
def api_keyword_rewrite_pr(request: Request, slug: str, body: _KeywordRewriteBody) -> JSONResponse:
    """Rewrite one page's title and description to answer a query it already ranks for, in a PR.

    This closes the loop the rest of the product exists for: Search Console names the query AND
    the page, `_rewrite_for_query` rewrites the two lines a searcher actually reads, and the
    customer gets a pull request on their own repository. The page keeps its subject — it already
    ranks, so the content is right — only its presentation changes.

    Deliberately narrower than the anomaly corrector in three ways, because no crawl backs it:
    the target file comes from the repo route map ALONE (never an AI file picker), the values are
    written by the model so the PR says so and never auto-merges, and a page whose title is
    assembled rather than written is refused instead of guessed at.
    """
    proj = _db_project_or_404(request, slug)
    user = getattr(request.state, "user", None)
    if not user:
        return JSONResponse({"ok": False, "error": "Session expirée."}, status_code=401)
    query = (body.query or "").strip()
    page_url = (body.url or "").strip()
    if not query or not page_url:
        return JSONResponse({"ok": False, "error": "Requête ou page manquante."}, status_code=400)
    if not _same_site_url(page_url, str(proj.base_url or "")):
        return JSONResponse({"ok": False, "error": "Cette page n'appartient pas au site du projet."}, status_code=400)

    cfg = _project_github_cfg(proj)
    if not cfg["repo"]:
        return JSONResponse({"ok": False, "needs_setup": True, "error": "Aucun dépôt GitHub connecté à ce projet."}, status_code=400)
    token, source = _effective_user_connection_value(user_id=str(user.id), key="GITHUB_TOKEN")
    if not token or source != "user":
        return JSONResponse({"ok": False, "error": "GitHub non connecté."}, status_code=400)
    retry_after = _rate_limit_retry_after(bucket="github_fix_user", subject=str(getattr(user, "id", "")), limit=20, window_s=60 * 60)
    if isinstance(retry_after, int):
        return JSONResponse({"ok": False, "error": f"Trop de requêtes. Réessaie dans {_format_retry_after(retry_after)}."}, status_code=429, headers={"Retry-After": str(retry_after)})
    gate_ok, gate_msg, gate_max_files, gate_model = _correction_gate(user)
    if not gate_ok:
        return JSONResponse({"ok": False, "error": gate_msg, "billing_url": "/billing"}, status_code=402)
    repo_parts = _github_repo_parts(cfg["repo"])
    if repo_parts is None:
        return JSONResponse({"ok": False, "needs_setup": True, "error": "Configuration GitHub invalide."}, status_code=400)
    owner, repo_name = repo_parts
    branch = cfg["branch"]
    if not _github_branch_allowed(branch):
        return JSONResponse({"ok": False, "error": "Branche GitHub invalide."}, status_code=400)

    # One open PR per PAGE, not per query: two queries pointing at the same page rewrite the same
    # two lines, and the second PR would conflict with the first.
    _open_pr = _open_pr_for_issue(
        project_id=str(proj.id), issue_key=_KEYWORD_REWRITE_KEY, url=page_url,
        owner=owner, repo_name=repo_name, token=token,
    )
    if _open_pr:
        return JSONResponse({"ok": False, "duplicate": True, "pr_url": _open_pr, "error": (
            "Une PR de réécriture est déjà ouverte pour cette page et attend ta revue. Une "
            "seconde toucherait les mêmes lignes, donc un conflit. Merge ou ferme celle-ci "
            "d'abord."
        )}, status_code=409)

    try:
        tree_data = _github_api_get(_github_api_path("repos", owner, repo_name, "git", "trees", branch), token=token, params={"recursive": "1"}, timeout_s=20)
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Lecture du dépôt impossible : {e}"}, status_code=400)
    all_paths = [
        item["path"] for item in (tree_data.get("tree") or [])
        if isinstance(item, dict) and item.get("type") == "blob" and _github_file_path_allowed(str(item.get("path") or ""))
    ]
    idx = repo_index.build_repo_index(all_paths)
    # The route map answers URL to source deterministically. Shared files are dropped for the
    # same reason the length families drop them: a title written into a layout becomes EVERY
    # page's title. And when the map cannot name the file we stop, rather than let an AI picker
    # choose where an editorial rewrite lands.
    targets = [
        path for path in repo_index.route_files(idx, page_url)
        if path in all_paths and not repo_index.is_shared_path(idx, path)
    ][:max(1, min(int(gate_max_files or 1), 3))]
    logger.info("[keywords] rewrite-pr %s %s -> %s", slug, page_url, targets or "aucun fichier")
    if not targets:
        return JSONResponse({"ok": False, "error": (
            "Le fichier source de cette page n'a pas pu être identifié dans le dépôt. Vérifie "
            "que le dépôt connecté contient bien le code du site."
        )}, status_code=422)

    from datetime import datetime as _dt
    try:
        ref_data = _github_api_get(_github_ref_api_path(owner, repo_name, branch), token=token)
        base_sha = ref_data["object"]["sha"]
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Impossible de lire la branche {branch} : {e}"}, status_code=400)
    fix_branch = f"seo-keyword/{_safe_github_branch_suffix(query)}-{_dt.utcnow().strftime('%Y%m%d-%H%M%S')}"
    try:
        _github_api_post(_github_api_path("repos", owner, repo_name, "git", "refs"), token=token, json_body={"ref": f"refs/heads/{fix_branch}", "sha": base_sha})
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Impossible de créer la branche : {e}"}, status_code=400)

    site_name = str(proj.site_name or slug)
    issue_label = f"Réécriture du titre et de la description pour « {query} »"
    file_state: dict[str, dict[str, str]] = {}
    patched_files, skipped, _targets, ai_files = _deep_patch_issue_files(
        owner=owner, repo_name=repo_name, branch=branch, token=token, fix_branch=fix_branch,
        all_paths=all_paths, issue_key=_KEYWORD_REWRITE_KEY, issue_label=issue_label,
        impacted_urls=[page_url], site_name=site_name, file_state=file_state,
        max_files=len(targets), model_override=gate_model, targets_override=targets,
        link_rewriter=(lambda raw: _rewrite_for_query(
            raw, query=query, url=page_url, site_name=site_name, model_override=gate_model,
        )),
        # No AI fallback: a page whose title is assembled from parts has no literal to swap, and
        # handing the whole file to a free-form patch is how a shared template gets rewritten.
        rewriter_ai_fallback=False,
        rewriter_is_ai=True,
        index=idx,
    )
    if not patched_files:
        return JSONResponse({"ok": False, "error": (
            f"Aucune réécriture appliquée sur {', '.join(targets)}. Le titre et la description de "
            "cette page sont probablement assemblés (variable, gabarit) plutôt qu'écrits dans le "
            "fichier : ils ne peuvent pas être remplacés sans réécrire la logique de la page."
        ), "skipped": skipped}, status_code=422)

    pr_title = f"seo(mots-clés) : titre et description pour « {query} »"
    pr_body = (
        "## Réécriture de snippet pour une requête déjà positionnée\n\n"
        f"**Requête :** {query}\n"
        f"**Page :** {page_url}\n"
        f"**Fichiers modifiés :** {len(patched_files)}\n\n"
        + "\n".join(f"- `{p}`" for p in patched_files)
        + "\n\nCette page ressort déjà sur cette requête dans Search Console : son sujet est le "
          "bon. Ce qui est réécrit ici, c'est ce qu'un internaute lit avant de cliquer — le "
          "titre et la meta description."
        + _fix_nature_note(True, _KEYWORD_REWRITE_KEY)
        + f"\n\nGénéré par [SEO Agent](https://noyaru.com) pour **{site_name}**."
    )
    try:
        pr_data = _github_api_post(_github_api_path("repos", owner, repo_name, "pulls"), token=token, json_body={"title": pr_title, "body": pr_body, "head": fix_branch, "base": branch})
        pr_url = pr_data.get("html_url", "")
        pr_number = pr_data.get("number", 0)
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Erreur lors de la création de la PR : {e}"}, status_code=400)

    # Never auto-merged, whatever the project's mode: every value in this diff was written by the
    # model, and a title is the most editorial thing this corrector touches.
    try:
        _note = json.dumps({"pr_url": pr_url, "pr_number": int(pr_number) if pr_number else 0,
                            "branch": fix_branch, "files": patched_files, "query": query,
                            "keyword": True}, ensure_ascii=False)
        with DB.session() as _db:
            _ex = _db.scalar(select(IssueTask).where(
                IssueTask.project_id == proj.id, IssueTask.issue_key == _KEYWORD_REWRITE_KEY,
                IssueTask.url == page_url))
            if _ex:
                _ex.status = "in_progress"
                _ex.issue_label = issue_label
                _ex.note = _note
            else:
                _db.add(IssueTask(
                    project_id=str(proj.id), user_id=str(getattr(user, "id", "") or ""),
                    issue_key=_KEYWORD_REWRITE_KEY, issue_label=issue_label, crawl_ts="",
                    url=page_url, status="in_progress", severity="notice", note=_note,
                ))
            _db.commit()
    except Exception:
        pass

    # Every file here carries model-written text, so every file costs one correction: the same
    # unit as the anomaly corrector, one file written by the model.
    _correction_charge(user, len(ai_files))

    return JSONResponse({
        "ok": True, "pr_url": pr_url, "pr_number": pr_number, "branch": fix_branch,
        "files": patched_files, "files_count": len(patched_files), "query": query,
    })


def _competitor_has_access(db, *, user_id: str) -> bool:
    """Pro and above. Owner's decision, 2026-08-29.

    One step above the backlink "Opportunités" gate (`_opp_has_access`, Solo+): a rival crawl
    spends worker slot-time on a site that is not the customer's, and the retargeting it feeds
    is the part of the product no competitor can copy.
    """
    plan_key = billing.effective_plan_key(db, user_id=str(user_id))
    return billing.plan_rank(plan_key) >= billing.plan_rank("pro")


def _competitor_domain(url: str) -> str:
    from urllib.parse import urlparse
    host = urlparse(url if "://" in url else f"https://{url}").netloc.lower().strip()
    return host.removeprefix("www.")


def _competitor_rows(db, project_id: str) -> list[Any]:
    return list(db.scalars(
        select(CompetitorSite)
        .where(CompetitorSite.project_id == str(project_id))
        .order_by(CompetitorSite.created_at.asc())
    ))


def _own_pages_for_project(runs_dir: Path, slug: str) -> tuple[list[dict[str, Any]], str]:
    """The customer's own pages, from their latest crawl that actually has a report."""
    crawls = dash.list_project_crawls(runs_dir, slug)
    for ts in reversed(crawls):
        report = dash.load_report_json(runs_dir, slug, ts)
        pages = report.get("pages") if isinstance(report, dict) else None
        if isinstance(pages, list) and pages:
            return [p for p in pages if isinstance(p, dict)], ts
    return [], ""


@app.get("/projects/{slug}/competitors", response_class=HTMLResponse)
def project_competitors(request: Request, slug: str,
                        msg: str | None = None, err: str | None = None) -> HTMLResponse:
    """What rivals build pages about, and which of those subjects this site already answers.

    Four states, and they lead to different actions: no access, no rival added, a rival added
    but never crawled, and a comparison. An empty table would conflate the last three.
    """
    proj_row = _db_project_or_404(request, slug)
    user = getattr(request.state, "user", None)
    runs_dir = _runs_dir_for_request(request)
    own_pages, own_crawl_ts = _own_pages_for_project(runs_dir, slug)

    with DB.session() as db:
        has_access = bool(user) and (
            bool(getattr(user, "is_admin", False))
            or _competitor_has_access(db, user_id=str(getattr(user, "id", "")))
        )
        rows = _competitor_rows(db, str(proj_row.id)) if has_access else []
        competitors = [{
            "id": str(r.id), "domain": r.domain, "base_url": r.base_url, "status": r.status,
            "pages_count": int(r.pages_count or 0), "error": r.error or "",
            "last_crawled_at": r.last_crawled_at.strftime("%d/%m/%Y") if r.last_crawled_at else "",
            "pages": r.pages if isinstance(r.pages, list) else [],
        } for r in rows]

    findings: list[dict[str, Any]] = []
    for comp in competitors:
        if comp["status"] != "ready" or not comp["pages"]:
            continue
        for finding in competitors_mod.compare(own_pages, comp["pages"], limit=25):
            finding["competitor_domain"] = comp["domain"]
            findings.append(finding)
    # Uncovered first, as the engine orders them: a subject nobody here answers is the more
    # interesting finding, even though it is the one the product will not act on.
    findings.sort(key=lambda f: (f["covered"], -f["match_score"]))
    summary = competitors_mod.summarise(findings)

    github_cfg = _project_github_cfg(proj_row)
    resp = templates.TemplateResponse(
        "competitors.html",
        {
            "request": request,
            "project": {"slug": proj_row.slug, "site_name": proj_row.site_name,
                        "base_url": proj_row.base_url},
            "slug": slug,
            "has_access": has_access,
            "competitors": competitors,
            "findings": findings[:100],
            "summary": summary,
            "own_pages_count": len(own_pages),
            "own_crawl_ts": own_crawl_ts,
            "max_competitors": _COMPETITOR_MAX_PER_PROJECT,
            "max_pages": _COMPETITOR_MAX_PAGES,
            "refresh_days": _COMPETITOR_REFRESH_DAYS,
            "github_cfg": github_cfg,
            # Every refusal on this page redirects with a message. Without these two the
            # customer whose rival was refused would see the page redraw and say nothing.
            "msg": msg,
            "err": err,
        },
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.post("/projects/{slug}/competitors/add")
def project_competitor_add(request: Request, slug: str, url: str = Form(default="")) -> RedirectResponse:
    proj = _db_project_or_404(request, slug)
    user = getattr(request.state, "user", None)
    page = f"/projects/{slug}/competitors"
    if not user:
        return RedirectResponse(url="/auth/login", status_code=303)

    raw = (url or "").strip()
    if raw and "://" not in raw:
        raw = f"https://{raw}"
    domain = _competitor_domain(raw)
    if not domain:
        return RedirectResponse(url=_path_with_flash(page, err="Adresse invalide."), status_code=303)
    if domain == _competitor_domain(str(proj.base_url or "")):
        return RedirectResponse(url=_path_with_flash(
            page, err="C'est ton propre site : compare-le à un concurrent."), status_code=303)
    # Same guard as a project's crawl target: no private host, no exotic port, no IP literal.
    validation_err = _validate_public_crawl_target(raw)
    if validation_err:
        return RedirectResponse(url=_path_with_flash(page, err=f"Cible refusée : {validation_err}"),
                                status_code=303)

    with DB.session() as db:
        if not (bool(getattr(user, "is_admin", False))
                or _competitor_has_access(db, user_id=str(user.id))):
            return RedirectResponse(url=_path_with_flash(page, err="Plan Pro+ requis"), status_code=303)
        rows = _competitor_rows(db, str(proj.id))
        if any(r.domain == domain for r in rows):
            return RedirectResponse(url=_path_with_flash(
                page, msg="Ce concurrent est déjà suivi."), status_code=303)
        if len(rows) >= _COMPETITOR_MAX_PER_PROJECT:
            return RedirectResponse(url=_path_with_flash(page, err=(
                f"Maximum {_COMPETITOR_MAX_PER_PROJECT} concurrents par projet : chacun est un "
                "crawl, et le temps de worker est la ressource rare.")), status_code=303)
        db.add(CompetitorSite(project_id=str(proj.id), user_id=str(user.id),
                              domain=domain, base_url=raw, status="new"))
        db.commit()
    return RedirectResponse(url=page, status_code=303)


@app.post("/projects/{slug}/competitors/remove")
def project_competitor_remove(request: Request, slug: str,
                              competitor_id: str = Form(default="")) -> RedirectResponse:
    proj = _db_project_or_404(request, slug)
    if not getattr(request.state, "user", None):
        return RedirectResponse(url="/auth/login", status_code=303)
    cid = (competitor_id or "").strip()
    if cid:
        with DB.session() as db:
            row = db.get(CompetitorSite, cid)
            # Scoped to the project in the URL, whose ownership was already checked: an id alone
            # must never be enough to touch another account's row.
            if row and str(row.project_id) == str(proj.id):
                db.delete(row)
                db.commit()
    return RedirectResponse(url=f"/projects/{slug}/competitors", status_code=303)


@app.post("/projects/{slug}/competitors/analyze")
def project_competitor_analyze(request: Request, slug: str,
                               competitor_id: str = Form(default="")) -> RedirectResponse:
    """Queue the rival crawl. Bounded at queue time, and never twice at once."""
    proj = _db_project_or_404(request, slug)
    user = getattr(request.state, "user", None)
    page = f"/projects/{slug}/competitors"
    if not user:
        return RedirectResponse(url="/auth/login", status_code=303)
    retry_after = _rate_limit_retry_after(bucket="competitor_crawl_user",
                                          subject=str(getattr(user, "id", "")), limit=10, window_s=60 * 60)
    if isinstance(retry_after, int):
        return RedirectResponse(url=_path_with_flash(page, err=(
            f"Trop d'analyses lancées. Réessaie dans {_format_retry_after(retry_after)}.")), status_code=303)

    with DB.session() as db:
        if not (bool(getattr(user, "is_admin", False))
                or _competitor_has_access(db, user_id=str(user.id))):
            return RedirectResponse(url=_path_with_flash(page, err="Plan Pro+ requis"), status_code=303)
        row = db.get(CompetitorSite, (competitor_id or "").strip())
        if not row or str(row.project_id) != str(proj.id):
            return RedirectResponse(url=_path_with_flash(page, err="Concurrent introuvable."), status_code=303)
        if row.status == "crawling":
            # A queued crawl the customer cannot see is a button they will press again.
            return RedirectResponse(url=_path_with_flash(
                page, msg="L'analyse de ce concurrent est déjà en cours."), status_code=303)
        cid, domain = str(row.id), row.domain
        row.status = "crawling"
        row.error = None
        db.commit()

    job = Job(id=str(uuid.uuid4()), status="queued", created_at=time.time())
    job.result = {"type": "competitor", "user_id": str(user.id), "competitor_id": cid,
                  "slug": slug, "domain": domain}
    _save_job(job)
    with DB.session() as db:
        row = db.get(CompetitorSite, cid)
        if row is not None:
            row.last_job_id = job.id
            db.commit()
    return RedirectResponse(url=_path_with_flash(
        page, msg=f"Analyse de {domain} lancée (jusqu'à {_COMPETITOR_MAX_PAGES} pages)."),
        status_code=303)


@app.post("/cron/refresh-competitors")
def cron_refresh_competitors(request: Request) -> JSONResponse:
    """Re-crawl every rival whose last pass is older than the refresh window.

    On demand AND monthly, the owner's answer: the button keeps the customer in control, and
    this keeps the page from quietly describing a site as it was six months ago.
    """
    cron_secret = str(os.environ.get("CRON_SECRET") or "").strip()
    if not cron_secret:
        return JSONResponse({"ok": False, "error": "CRON_SECRET non configuré"}, status_code=500)
    auth = request.headers.get("Authorization", "")
    token = auth[len("Bearer "):].strip() if auth.startswith("Bearer ") else auth.strip()
    if not token or not hmac.compare_digest(token, cron_secret):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    cutoff = datetime.now(timezone.utc) - timedelta(days=_COMPETITOR_REFRESH_DAYS)
    queued: list[str] = []
    with DB.session() as db:
        rows = list(db.scalars(select(CompetitorSite).where(CompetitorSite.status != "crawling")))
        for row in rows:
            last = row.last_crawled_at
            if last is not None:
                if last.tzinfo is None:
                    last = last.replace(tzinfo=timezone.utc)
                if last > cutoff:
                    continue
            # A rival whose plan no longer includes the feature stops being refreshed: the crawl
            # costs worker time either way, and it would feed a page the customer cannot open.
            if not _competitor_has_access(db, user_id=str(row.user_id)):
                continue
            job = Job(id=str(uuid.uuid4()), status="queued", created_at=time.time())
            job.result = {"type": "competitor", "user_id": str(row.user_id),
                          "competitor_id": str(row.id), "domain": row.domain}
            _save_job(job)
            row.status = "crawling"
            row.last_job_id = job.id
            queued.append(row.domain)
        db.commit()
    logger.info("[competitors] refresh: %d crawl(s) mis en file", len(queued))
    return JSONResponse({"ok": True, "queued": len(queued), "domains": queued[:20]})


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

    # Fallback to stored crawl CSV if live returned ok but empty items
    if live_payload.get("ok") and not live_payload.get("items"):
        fallback_items = _crawl_items_fallback(runs_dir, slug, src, dimension, fetch_limit)
        if fallback_items:
            live_payload = dict(live_payload)
            live_payload["items"] = fallback_items
            live_payload["live"] = False
            live_payload["fallback"] = True

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

    _CSV_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
    content_type = str(file.content_type or "").lower().split(";")[0].strip()
    _ALLOWED_CSV_TYPES = {"text/csv", "text/plain", "application/csv", "application/octet-stream"}
    if content_type and content_type not in _ALLOWED_CSV_TYPES:
        return RedirectResponse(
            url=f"/projects/{slug}/backlinks?crawl={quote(ts)}&err={quote('Type de fichier invalide (CSV attendu)')}",
            status_code=303,
        )

    try:
        content = await file.read(_CSV_MAX_BYTES + 1)
    except Exception as e:
        return RedirectResponse(url=f"/projects/{slug}/backlinks?crawl={quote(ts)}&err={quote(str(e))}", status_code=303)

    if len(content) > _CSV_MAX_BYTES:
        return RedirectResponse(
            url=f"/projects/{slug}/backlinks?crawl={quote(ts)}&err={quote('Fichier trop volumineux (max 10 Mo)')}",
            status_code=303,
        )

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
                        "imported_at": datetime.now(timezone.utc).isoformat(timespec="seconds") + "Z",
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

    imported_at = datetime.now(timezone.utc).isoformat(timespec="seconds") + "Z"
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


# ---------------------------------------------------------------------------
# Backlink Opportunities (premium feature — Solo+ plan required)
# ---------------------------------------------------------------------------

def _score_opportunity(source: str, title: str, url: str, snippet: str = "", query: str = "") -> int:
    score = 20
    text = f"{title} {snippet}".lower()
    url_lower = url.lower()

    intent_kw = [
        "how", "best", "recommend", "recommendation", "alternative", "tool", "tools",
        "review", "compare", "worth it", "anyone", "help", "question", "guide",
        "make money", "earn", "beginner", "start", "need advice",
    ]
    score += sum(7 for kw in intent_kw if kw in text)

    bad_kw = ["coupon", "promo code", "download crack", "torrent", "nsfw"]
    score -= sum(12 for kw in bad_kw if kw in text)

    if source == "reddit":
        score += 12
    if "/comments/" in url_lower or "reddit.com/r/" in url_lower:
        score += 10
    if "quora.com" in url_lower:
        score += 8
    if "medium.com" in url_lower:
        score += 3

    if query:
        query_words = [w for w in query.lower().split() if len(w) > 3]
        matches = sum(1 for w in query_words if w in text)
        score += min(20, matches * 4)

    return max(0, min(100, round(score)))


def _opp_has_access(db_session, *, user_id: str) -> bool:
    plan_key = billing.effective_plan_key(db_session, user_id=user_id)
    return billing.plan_rank(plan_key) >= billing.plan_rank("solo")


@app.get("/projects/{slug}/backlinks/opportunities", response_class=HTMLResponse)
def project_backlinks_opportunities(
    request: Request,
    slug: str,
    q: str | None = None,
    status: str | None = None,
    page: int = 1,
    msg: str | None = None,
    err: str | None = None,
) -> HTMLResponse:
    proj = _db_project_or_404(request, slug)
    user = request.state.user

    page = max(1, page)
    page_size = 20

    with DB.session() as db:
        has_access = _opp_has_access(db, user_id=str(user.id))
        plan_key = billing.effective_plan_key(db, user_id=str(user.id))
        search_remaining = billing.remaining_quota(db, user_id=str(user.id), metric="backlink_searches_month")
        reply_remaining = billing.remaining_quota(db, user_id=str(user.id), metric="backlink_replies_month")

        opportunities: list[BacklinkOpportunity] = []
        total = 0

        if has_access:
            base_q = (
                select(BacklinkOpportunity)
                .where(
                    BacklinkOpportunity.project_id == str(proj.id),
                    BacklinkOpportunity.user_id == str(user.id),
                )
            )
            if q:
                base_q = base_q.where(BacklinkOpportunity.title.ilike(f"%{q}%"))
            status = (status or "").strip().lower() or None
            if status and status in {"new", "contacted", "won", "lost"}:
                base_q = base_q.where(BacklinkOpportunity.status == status)
            else:
                status = None

            count_q = select(func.count()).select_from(base_q.subquery())
            total = int(db.scalar(count_q) or 0)
            offset = (page - 1) * page_size
            rows = db.scalars(
                base_q.order_by(
                    BacklinkOpportunity.opportunity_score.desc(),
                    BacklinkOpportunity.created_at.desc(),
                ).offset(offset).limit(page_size)
            )
            opportunities = list(rows)

    project_ctx = {"slug": slug, "site_name": proj.site_name, "base_url": proj.base_url}
    pages_total = max(1, math.ceil(total / page_size))

    auto_cfg = _backlinks_auto_cfg(proj.settings or {})

    queue_items: list[BacklinkOpportunity] = []
    if has_access:
        with DB.session() as db2:
            queue_items = list(db2.scalars(
                select(BacklinkOpportunity)
                .where(
                    BacklinkOpportunity.project_id == str(proj.id),
                    BacklinkOpportunity.user_id == str(user.id),
                    BacklinkOpportunity.queue_status.in_(["pending", "approved"]),
                )
                .order_by(BacklinkOpportunity.opportunity_score.desc(), BacklinkOpportunity.created_at.desc())
            ))

    resp = templates.TemplateResponse(
        "backlinks_opportunities.html",
        {
            "request": request,
            "project": project_ctx,
            "slug": slug,
            "has_access": has_access,
            "plan_key": plan_key,
            "opportunities": opportunities,
            "q": q or "",
            "status_filter": status or "",
            "page": page,
            "pages_total": pages_total,
            "total": total,
            "page_size": page_size,
            "search_remaining": search_remaining,
            "reply_remaining": reply_remaining,
            "auto_cfg": auto_cfg,
            "queue_items": queue_items,
            "msg": msg or "",
            "err": err or "",
        },
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.post("/api/projects/{slug}/backlinks/search")
async def api_backlinks_search(request: Request, slug: str) -> JSONResponse:
    _db_project_or_404(request, slug)
    user = request.state.user

    with DB.session() as db:
        if not _opp_has_access(db, user_id=str(user.id)):
            return JSONResponse({"ok": False, "error": "Fonctionnalité réservée au plan Solo+."}, status_code=403)

        allowed, remaining = billing.ensure_within_quota(
            db, user_id=str(user.id), metric="backlink_searches_month", planned_amount=1
        )
        if not allowed:
            return JSONResponse(
                {"ok": False, "error": f"Quota mensuel de recherches atteint ({remaining or 0} restant)."},
                status_code=429,
            )

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "Corps JSON invalide"}, status_code=400)

    raw_query = str(body.get("query") or "").strip()
    source = str(body.get("source") or "google").strip().lower()
    if source not in {"reddit", "google"}:
        source = "google"
    if not raw_query:
        return JSONResponse({"ok": False, "error": "Requête vide"}, status_code=400)

    serpapi_key = str(os.environ.get("SERPAPI_API_KEY") or os.environ.get("SERPAPI_KEY") or "").strip()
    if not serpapi_key:
        return JSONResponse({"ok": False, "error": "SERPAPI_API_KEY non configurée"}, status_code=500)

    search_q = f"site:reddit.com {raw_query}" if source == "reddit" else raw_query

    try:
        resp = requests.get(
            "https://serpapi.com/search.json",
            params={"engine": "google", "q": search_q, "api_key": serpapi_key, "num": "10", "hl": "en"},
            timeout=20,
        )
        data = resp.json()
    except Exception as exc:
        return JSONResponse({"ok": False, "error": f"Erreur SerpAPI: {exc}"}, status_code=502)

    if not resp.ok:
        err_msg = str((data or {}).get("error") or f"SerpAPI {resp.status_code}")
        return JSONResponse({"ok": False, "error": err_msg}, status_code=502)

    items = []
    for r in data.get("organic_results") or []:
        url = str(r.get("link") or "").strip()
        if not url:
            continue
        title = str(r.get("title") or "Untitled")
        snippet = str(r.get("snippet") or "")
        score = _score_opportunity(source, title, url, snippet, raw_query)
        items.append({"source": source, "title": title, "url": url, "snippet": snippet, "opportunity_score": score})

    items.sort(key=lambda x: x["opportunity_score"], reverse=True)

    with DB.session() as db:
        billing.usage_add(db, user_id=str(user.id), metric="backlink_searches_month", amount=1)

    return JSONResponse({"ok": True, "data": items})


@app.post("/api/projects/{slug}/backlinks/generate-reply")
async def api_backlinks_generate_reply(request: Request, slug: str) -> JSONResponse:
    _db_project_or_404(request, slug)
    user = request.state.user

    with DB.session() as db:
        if not _opp_has_access(db, user_id=str(user.id)):
            return JSONResponse({"ok": False, "error": "Fonctionnalité réservée au plan Solo+."}, status_code=403)

        allowed, remaining = billing.ensure_within_quota(
            db, user_id=str(user.id), metric="backlink_replies_month", planned_amount=1
        )
        if not allowed:
            return JSONResponse(
                {"ok": False, "error": f"Quota mensuel de génération atteint ({remaining or 0} restant)."},
                status_code=429,
            )

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "Corps JSON invalide"}, status_code=400)

    platform = str(body.get("platform") or "").strip() or "web"
    opp_title = str(body.get("opportunityTitle") or "").strip()
    opp_url = str(body.get("opportunityUrl") or "").strip()
    target_url = str(body.get("targetArticleUrl") or "").strip()

    if not opp_title or not target_url:
        return JSONResponse({"ok": False, "error": "Titre et URL cible requis"}, status_code=400)

    if not _ai_configured():
        return JSONResponse({"ok": False, "error": "Génération IA momentanément indisponible."}, status_code=503)

    system_prompt = (
        "Tu es un expert en netlinking et en community management. "
        "Tu rédiges des réponses naturelles et utiles pour obtenir des backlinks. "
        "Réponds toujours dans la langue de la discussion (français ou anglais selon le contexte). "
        "Sois authentique, apporte de la valeur, et mentionne le lien de façon naturelle."
    )
    user_prompt = (
        f"Plateforme : {platform}\n"
        f"Discussion : {opp_title}\n"
        f"URL de la discussion : {opp_url}\n"
        f"Article à promouvoir : {target_url}\n\n"
        "Rédige une réponse naturelle (150-250 mots) qui apporte de la valeur à la discussion "
        "et mentionne l'article de façon pertinente avec son URL."
    )

    ai_errors: list[str] = []
    reply_text = _ai_generate_text(
        system=system_prompt, user_msg=user_prompt, max_tokens=700, temperature=0.7,
        error_sink=ai_errors,
    )
    if not reply_text:
        # Provider/model details are admin-only; users get a generic message.
        if bool(getattr(user, "is_admin", False)) and ai_errors:
            err_msg = "Génération IA indisponible : " + " · ".join(ai_errors[:2])
        else:
            err_msg = "Génération IA momentanément indisponible. Réessaie dans un instant."
        return JSONResponse({"ok": False, "error": err_msg}, status_code=502)

    with DB.session() as db:
        billing.usage_add(db, user_id=str(user.id), metric="backlink_replies_month", amount=1)

    return JSONResponse({"ok": True, "reply": reply_text})


@app.post("/projects/{slug}/backlinks/opportunities/save")
async def backlinks_opportunity_save(
    request: Request,
    slug: str,
    source: str = Form(default=""),
    title: str = Form(default=""),
    url: str = Form(default=""),
    snippet: str = Form(default=""),
    opportunity_score: int = Form(default=0),
    reply: str = Form(default=""),
    target_url: str = Form(default=""),
) -> RedirectResponse:
    proj = _db_project_or_404(request, slug)
    user = request.state.user

    with DB.session() as db:
        if not _opp_has_access(db, user_id=str(user.id)):
            return RedirectResponse(
                url=f"/projects/{slug}/backlinks/opportunities?err={quote('Plan Solo+ requis')}",
                status_code=303,
            )

    url = url.strip()
    title = title.strip()
    if not url or not title:
        return RedirectResponse(
            url=f"/projects/{slug}/backlinks/opportunities?err={quote('URL et titre requis')}",
            status_code=303,
        )

    with DB.session() as db:
        existing = db.scalar(
            select(BacklinkOpportunity).where(
                BacklinkOpportunity.project_id == str(proj.id),
                BacklinkOpportunity.url == url,
            )
        )
        if existing:
            existing.title = title
            existing.snippet = snippet.strip() or None
            existing.opportunity_score = max(0, min(100, opportunity_score))
            existing.reply = reply.strip() or None
            existing.target_url = target_url.strip() or None
            db.add(existing)
        else:
            opp = BacklinkOpportunity(
                project_id=str(proj.id),
                user_id=str(user.id),
                source=source.strip() or "web",
                title=title,
                url=url,
                snippet=snippet.strip() or None,
                opportunity_score=max(0, min(100, opportunity_score)),
                status="new",
                reply=reply.strip() or None,
                target_url=target_url.strip() or None,
            )
            db.add(opp)
        try:
            db.commit()
        except IntegrityError:
            db.rollback()

    return RedirectResponse(
        url=f"/projects/{slug}/backlinks/opportunities?msg={quote('Opportunité sauvegardée')}",
        status_code=303,
    )


@app.post("/projects/{slug}/backlinks/opportunities/{opp_id}/delete")
def backlinks_opportunity_delete(request: Request, slug: str, opp_id: str) -> RedirectResponse:
    proj = _db_project_or_404(request, slug)
    user = request.state.user

    with DB.session() as db:
        opp = db.scalar(
            select(BacklinkOpportunity).where(
                BacklinkOpportunity.id == opp_id,
                BacklinkOpportunity.project_id == str(proj.id),
                BacklinkOpportunity.user_id == str(user.id),
            )
        )
        if opp:
            db.delete(opp)
            db.commit()

    return RedirectResponse(
        url=f"/projects/{slug}/backlinks/opportunities?msg={quote('Opportunité supprimée')}",
        status_code=303,
    )


@app.post("/projects/{slug}/backlinks/opportunities/{opp_id}/status")
def backlinks_opportunity_status(
    request: Request,
    slug: str,
    opp_id: str,
    status: str = Form(default="new"),
) -> RedirectResponse:
    proj = _db_project_or_404(request, slug)
    user = request.state.user
    status = (status or "new").strip().lower()
    if status not in {"new", "contacted", "won", "lost"}:
        status = "new"

    with DB.session() as db:
        opp = db.scalar(
            select(BacklinkOpportunity).where(
                BacklinkOpportunity.id == opp_id,
                BacklinkOpportunity.project_id == str(proj.id),
                BacklinkOpportunity.user_id == str(user.id),
            )
        )
        if opp:
            opp.status = status
            db.add(opp)
            db.commit()

    return RedirectResponse(
        url=f"/projects/{slug}/backlinks/opportunities?msg={quote('Statut mis à jour')}",
        status_code=303,
    )


def _backlinks_auto_cfg(proj_settings: dict) -> dict:
    cfg = (proj_settings or {}).get("backlinks_auto") or {}
    return {
        "enabled": bool(cfg.get("enabled", False)),
        "keywords": cfg.get("keywords") or [],
        "sources": cfg.get("sources") or ["reddit", "google"],
        "frequency": cfg.get("frequency", "daily"),
        "max_per_run": int(cfg.get("max_per_run", 10)),
        "auto_draft": bool(cfg.get("auto_draft", True)),
        "last_run": cfg.get("last_run"),
    }


# ---------------------------------------------------------------------------
# Backlink automation — API routes (settings, queue)
# ---------------------------------------------------------------------------

@app.post("/api/projects/{slug}/backlinks/auto-settings")
async def backlinks_auto_settings_save(request: Request, slug: str) -> JSONResponse:
    proj = _db_project_or_404(request, slug)
    user = request.state.user

    with DB.session() as db:
        if not _opp_has_access(db, user_id=str(user.id)):
            return JSONResponse({"ok": False, "error": "Plan Solo+ requis"}, status_code=403)

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "JSON invalide"}, status_code=400)

    enabled = bool(body.get("enabled", False))
    keywords_raw = str(body.get("keywords") or "").strip()
    keywords = [k.strip() for k in keywords_raw.splitlines() if k.strip()][:20]
    sources_raw = body.get("sources") or ["reddit", "google"]
    sources = [s for s in (sources_raw if isinstance(sources_raw, list) else []) if s in {"reddit", "google"}] or ["reddit", "google"]
    frequency = "weekly" if body.get("frequency") == "weekly" else "daily"
    max_per_run = max(1, min(20, int(body.get("max_per_run") or 10)))
    auto_draft = bool(body.get("auto_draft", True))

    with DB.session() as db:
        p = db.scalar(select(Project).where(Project.id == proj.id))
        if not p:
            return JSONResponse({"ok": False, "error": "Projet introuvable"}, status_code=404)
        settings = dict(p.settings or {})
        existing_cfg = settings.get("backlinks_auto") or {}
        settings["backlinks_auto"] = {
            "enabled": enabled,
            "keywords": keywords,
            "sources": sources,
            "frequency": frequency,
            "max_per_run": max_per_run,
            "auto_draft": auto_draft,
            "last_run": existing_cfg.get("last_run"),
        }
        p.settings = settings
        db.add(p)
        db.commit()

    return JSONResponse({"ok": True})


@app.post("/api/projects/{slug}/backlinks/queue/{opp_id}/approve")
def backlinks_queue_approve(request: Request, slug: str, opp_id: str) -> JSONResponse:
    proj = _db_project_or_404(request, slug)
    user = request.state.user
    with DB.session() as db:
        opp = db.scalar(select(BacklinkOpportunity).where(
            BacklinkOpportunity.id == opp_id,
            BacklinkOpportunity.project_id == str(proj.id),
            BacklinkOpportunity.user_id == str(user.id),
        ))
        if not opp:
            return JSONResponse({"ok": False, "error": "Introuvable"}, status_code=404)
        opp.queue_status = "approved"
        db.add(opp)
        db.commit()
    return JSONResponse({"ok": True})


@app.post("/api/projects/{slug}/backlinks/queue/{opp_id}/reject")
def backlinks_queue_reject(request: Request, slug: str, opp_id: str) -> JSONResponse:
    proj = _db_project_or_404(request, slug)
    user = request.state.user
    with DB.session() as db:
        opp = db.scalar(select(BacklinkOpportunity).where(
            BacklinkOpportunity.id == opp_id,
            BacklinkOpportunity.project_id == str(proj.id),
            BacklinkOpportunity.user_id == str(user.id),
        ))
        if not opp:
            return JSONResponse({"ok": False, "error": "Introuvable"}, status_code=404)
        opp.queue_status = "rejected"
        db.add(opp)
        db.commit()
    return JSONResponse({"ok": True})


@app.post("/api/projects/{slug}/backlinks/queue/{opp_id}/mark-sent")
def backlinks_queue_mark_sent(request: Request, slug: str, opp_id: str) -> JSONResponse:
    """Mark an opportunity as manually sent (copy+open flow — no direct API posting)."""
    proj = _db_project_or_404(request, slug)
    user = request.state.user
    with DB.session() as db:
        opp = db.scalar(select(BacklinkOpportunity).where(
            BacklinkOpportunity.id == opp_id,
            BacklinkOpportunity.project_id == str(proj.id),
            BacklinkOpportunity.user_id == str(user.id),
        ))
        if not opp:
            return JSONResponse({"ok": False, "error": "Introuvable"}, status_code=404)
        opp.queue_status = "posted"
        opp.posted_at = datetime.now(timezone.utc)
        opp.status = "contacted"
        db.add(opp)
        db.commit()
    return JSONResponse({"ok": True})


# ---------------------------------------------------------------------------
# Cron Render — vérification automatique des backlinks
# ---------------------------------------------------------------------------

def _notify_operator(*, subject: str, text_body: str) -> None:
    """Operator notification (backlink cron), through the same transport as everything else.

    This used to call SendGrid's API directly with its own SENDGRID_API_KEY, bypassing
    _send_email entirely — so it was a second vendor lock nobody would have found until it,
    too, went silent. It swallowed every exception, which is defensible for a notification
    but means it can never explain itself; the transport layer logs the provider's own words.
    """
    to_email = str(_safe_env("NOTIFICATION_EMAIL") or _support_email() or "").strip()
    if not to_email:
        return
    try:
        _send_email(to_addr=to_email, subject=subject, body=text_body)
    except Exception as e:
        # Never let a failed notification break the cron run it is reporting on.
        logger.warning("[MAIL] operator notification failed: %s: %s", type(e).__name__, e)


@app.get("/cron/check-backlinks")
def cron_check_backlinks(request: Request) -> JSONResponse:
    cron_secret = str(os.environ.get("CRON_SECRET") or "").strip()
    if not cron_secret:
        return JSONResponse({"ok": False, "error": "CRON_SECRET non configuré"}, status_code=500)
    auth = request.headers.get("Authorization", "")
    token = auth[len("Bearer "):].strip() if auth.startswith("Bearer ") else auth.strip()
    if not token or not hmac.compare_digest(token, cron_secret):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)

    with DB.session() as db:
        opps = list(db.scalars(
            select(BacklinkOpportunity).where(
                BacklinkOpportunity.status.in_(["won", "contacted"]),
                BacklinkOpportunity.target_url.isnot(None),
            )
        ))

    checked = 0
    newly_lost: list[dict[str, str]] = []
    check_errors: list[dict[str, str]] = []

    for opp in opps:
        target = str(opp.target_url or "").strip().lower()
        if not target:
            continue
        source_url = str(opp.url or "").strip()
        validation_err = _validate_public_crawl_target(source_url)
        if validation_err:
            check_errors.append({"url": source_url[:200], "error": validation_err})
            continue
        try:
            resp = requests.get(
                source_url,
                timeout=12,
                allow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0 (compatible; SEOAgentBot/1.0)"},
            )
            found = target in resp.text.lower()
            checked += 1
            if not found and str(opp.status) == "won":
                with DB.session() as db2:
                    row = db2.scalar(select(BacklinkOpportunity).where(BacklinkOpportunity.id == opp.id))
                    if row:
                        row.status = "lost"
                        db2.add(row)
                        db2.commit()
                newly_lost.append({
                    "title": str(opp.title or ""),
                    "url": str(opp.url or ""),
                    "target_url": target,
                })
        except Exception as exc:
            check_errors.append({"url": str(opp.url or ""), "error": str(exc)[:120]})

    if newly_lost:
        lines = "\n".join(
            f"- {item['title'][:70]}\n  Source : {item['url']}\n  Cible  : {item['target_url']}"
            for item in newly_lost
        )
        _notify_operator(
            subject=f"⚠️ {len(newly_lost)} backlink(s) perdu(s) — Agent SEO",
            text_body=(
                f"{len(newly_lost)} backlink(s) ne semblent plus actifs :\n\n{lines}\n\n"
                "Ces opportunités ont été passées au statut « perdu » automatiquement."
            ),
        )

    return JSONResponse({
        "ok": True,
        "checked": checked,
        "newly_lost": len(newly_lost),
        "errors": len(check_errors),
        "error_details": check_errors[:10],
    })


# ---------------------------------------------------------------------------
# Cron Render — recherche automatique d'opportunités de backlinks
# ---------------------------------------------------------------------------

def _cron_auth_check(request: Request) -> bool:
    cron_secret = str(os.environ.get("CRON_SECRET") or "").strip()
    if not cron_secret:
        return False
    auth = request.headers.get("Authorization", "")
    token = auth[len("Bearer "):].strip() if auth.startswith("Bearer ") else auth.strip()
    return bool(token) and hmac.compare_digest(token, cron_secret)


@app.get("/cron/auto-search-backlinks")
def cron_auto_search_backlinks(request: Request) -> JSONResponse:
    if not os.environ.get("CRON_SECRET"):
        return JSONResponse({"ok": False, "error": "CRON_SECRET non configuré"}, status_code=500)
    if not _cron_auth_check(request):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)

    serpapi_key = str(os.environ.get("SERPAPI_API_KEY") or os.environ.get("SERPAPI_KEY") or "").strip()
    ai_ready = _ai_configured()

    with DB.session() as db:
        all_projects = list(db.scalars(select(Project)))

    total_found = 0
    total_drafted = 0
    project_results: list[dict] = []

    for proj in all_projects:
        auto_cfg = _backlinks_auto_cfg(proj.settings or {})
        if not auto_cfg["enabled"] or not auto_cfg["keywords"]:
            continue

        last_run_str = auto_cfg.get("last_run")
        if last_run_str:
            try:
                last_run = datetime.fromisoformat(last_run_str)
                hours_since = (datetime.now(timezone.utc) - last_run).total_seconds() / 3600
                min_hours = 20 if auto_cfg["frequency"] == "daily" else 160
                if hours_since < min_hours:
                    continue
            except Exception:
                pass

        if not serpapi_key:
            continue

        user_id = str(proj.owner_user_id)
        found_this_proj = 0
        drafted_this_proj = 0

        for kw in auto_cfg["keywords"][:5]:
            for src in auto_cfg["sources"]:
                search_q = f"site:reddit.com {kw}" if src == "reddit" else kw
                try:
                    resp = requests.get(
                        "https://serpapi.com/search.json",
                        params={"engine": "google", "q": search_q, "api_key": serpapi_key, "num": "10", "hl": "en"},
                        timeout=20,
                    )
                    data = resp.json()
                    if not resp.ok:
                        continue
                except Exception:
                    continue

                for r in (data.get("organic_results") or []):
                    opp_url = str(r.get("link") or "").strip()
                    if not opp_url:
                        continue
                    title = str(r.get("title") or "Untitled")
                    snippet = str(r.get("snippet") or "")
                    score = _score_opportunity(src, title, opp_url, snippet, kw)

                    with DB.session() as db2:
                        existing = db2.scalar(
                            select(BacklinkOpportunity).where(
                                BacklinkOpportunity.project_id == str(proj.id),
                                BacklinkOpportunity.url == opp_url,
                            )
                        )
                        if existing:
                            continue
                        new_opp = BacklinkOpportunity(
                            project_id=str(proj.id),
                            user_id=user_id,
                            source=src,
                            title=title,
                            url=opp_url,
                            snippet=snippet or None,
                            opportunity_score=score,
                            status="new",
                            auto_found=True,
                            queue_status="pending" if auto_cfg["auto_draft"] else None,
                        )
                        db2.add(new_opp)
                        try:
                            db2.commit()
                        except Exception:
                            db2.rollback()
                            continue
                        opp_id = new_opp.id
                        found_this_proj += 1

                    if auto_cfg["auto_draft"] and ai_ready and proj.base_url:
                        target_url = str(proj.base_url or "").rstrip("/")
                        system_prompt = (
                            "Tu es un expert en netlinking. Tu rédiges des réponses naturelles et utiles "
                            "pour obtenir des backlinks. Sois authentique, apporte de la valeur, et mentionne "
                            "le lien de façon naturelle. Réponds dans la langue de la discussion."
                        )
                        user_prompt = (
                            f"Discussion : {title}\nURL : {opp_url}\nSite à promouvoir : {target_url}\n\n"
                            "Rédige une réponse (150-250 mots) qui apporte de la valeur et mentionne "
                            "naturellement l'URL du site."
                        )
                        try:
                            draft = _ai_generate_text(
                                system=system_prompt, user_msg=user_prompt,
                                max_tokens=700, temperature=0.7,
                            )
                            if draft:
                                with DB.session() as db3:
                                    opp3 = db3.scalar(select(BacklinkOpportunity).where(BacklinkOpportunity.id == opp_id))
                                    if opp3:
                                        opp3.reply = draft
                                        db3.add(opp3)
                                        db3.commit()
                                drafted_this_proj += 1
                        except Exception:
                            pass

                    if found_this_proj >= auto_cfg["max_per_run"]:
                        break
                if found_this_proj >= auto_cfg["max_per_run"]:
                    break
            if found_this_proj >= auto_cfg["max_per_run"]:
                break

        with DB.session() as db4:
            p4 = db4.scalar(select(Project).where(Project.id == proj.id))
            if p4:
                settings4 = dict(p4.settings or {})
                auto4 = dict(settings4.get("backlinks_auto") or {})
                auto4["last_run"] = datetime.now(timezone.utc).isoformat()
                settings4["backlinks_auto"] = auto4
                p4.settings = settings4
                db4.add(p4)
                db4.commit()

        total_found += found_this_proj
        total_drafted += drafted_this_proj
        project_results.append({"project": str(proj.slug), "found": found_this_proj, "drafted": drafted_this_proj})

    return JSONResponse({"ok": True, "total_found": total_found, "total_drafted": total_drafted, "projects": project_results})


@app.get("/cron/auto-post-backlinks")
def cron_auto_post_backlinks(request: Request) -> JSONResponse:
    """Deprecated — automated Reddit posting removed. Publication is now manual (copy+open flow)."""
    if not _cron_auth_check(request):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    return JSONResponse({"ok": True, "message": "Automated posting disabled. Use copy+open flow.", "posted": 0})


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
            _cfg_path = Path(str(job.config_path)).expanduser().resolve()
            _cfg_path.relative_to(REPO_ROOT.resolve())  # raises ValueError if outside REPO_ROOT
            # This is a bit brittle, relies on knowing the orchestrator's output structure.
            with open(_cfg_path, 'r', encoding='utf-8') as f:
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

    # If running: request cancellation. The worker subprocess loop polls DB and will SIGKILL.
    # If the worker is dead, _finalize_stale_job will auto-finalize after 30s (cancel_after_s).
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
        if not job.config_path:
            return RedirectResponse(url=f"/jobs/{job_id}", status_code=303)
        try:
            cfg = _resolve_request_config_path(request, job.config_path)
        except HTTPException:
            return RedirectResponse(url=_path_with_flash(f"/jobs/{job_id}", err="Fichier de configuration refusé."), status_code=303)
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

    # Auto-finalize orphaned jobs (e.g. cancel_requested whose worker died, or stale
    # running/queued jobs) directly from the live poll, so the frontend reloads without
    # requiring a manual page refresh. No-op for jobs active in this process.
    _finalize_stale_job(job)

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
    try:
        if plan_path.stat().st_size > 1024 * 1024:
            print(f"[FIXER] ERROR: Plan file too large: {plan_path}")
            return
    except Exception:
        return

    print(f"[FIXER] Applying corrections from: {plan_path}")
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    if not isinstance(plan, list):
        print("[FIXER] ERROR: Invalid plan format (expected a JSON list).")
        return
    if len(plan) > 500:
        print("[FIXER] ERROR: Plan contains too many corrections.")
        return

    backup_root = (plan_path.parent / f"corrections-backup-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}").resolve()
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
                logger.warning("[FIXER] Refusing to write outside allowed roots: %s", file_to_fix)
                continue

            if file_to_fix.suffix.lower() not in {".html", ".htm"}:
                logger.warning("[FIXER] Refusing to edit non-HTML file: %s", file_to_fix)
                continue

            issue_type = str(correction.get("issue_type") or "").strip()
            current_value = str(correction.get("current_value") or "")
            suggested_value = correction.get("suggested_value")
            if suggested_value is None:
                logger.warning("[FIXER] Missing suggested_value for %s", file_to_fix)
                continue
            suggested_value_str = str(suggested_value)

            if not file_to_fix.exists():
                logger.warning("[FIXER] File not found, cannot apply fix: %s", file_to_fix)
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
            logger.error("[FIXER] Failed to apply correction for %s: %s", correction.get("file_path"), e)


@app.post("/jobs/{job_id}/apply-corrections")
def apply_corrections(
    request: Request,
    job_id: str,
    plan_path: str = Form(...),
) -> RedirectResponse:
    user = _require_admin(request)
    job = _load_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Guardrail: only accept plans inside `seo-runs/` (prevents arbitrary file reads/writes via crafted form input).
    try:
        resolved_plan = _resolve_path_under_root(plan_path, DEFAULT_RUNS_DIR)
    except HTTPException:
        _audit_log(
            request,
            action="jobs.apply_corrections",
            status="blocked",
            user=user,
            target_type="job",
            target_id=job_id,
            meta={"reason": "plan_path_not_allowed"},
        )
        return RedirectResponse(
            url=_path_with_flash(f"/jobs/{job_id}", err="Plan de corrections refusé."),
            status_code=303,
        )
    if not resolved_plan.exists() or not resolved_plan.is_file():
        return RedirectResponse(
            url=_path_with_flash(f"/jobs/{job_id}", err="Plan de corrections introuvable."),
            status_code=303,
        )

    # For now, running this synchronously.
    # In a real app, you'd use the background_tasks or a proper worker queue.
    # background_tasks.add_task(_apply_corrections_worker, plan_path)
    _apply_corrections_worker(str(resolved_plan))
    _audit_log(
        request,
        action="jobs.apply_corrections",
        status="ok",
        user=user,
        target_type="job",
        target_id=job_id,
        meta={"plan_path": str(resolved_plan)},
    )

    return RedirectResponse(url=f"/jobs/{job_id}", status_code=303)


# ---------------------------------------------------------------------------
# Global error handlers
# ---------------------------------------------------------------------------

def _wants_json(request: Request) -> bool:
    accept = request.headers.get("accept", "")
    return "application/json" in accept and "text/html" not in accept


def _error_ctx(request: Request, status_code: int, detail: str) -> dict:
    return {
        "request": request,
        "app_name": _app_name(),
        "year": datetime.now(timezone.utc).year,
        "status_code": status_code,
        "detail": detail,
    }


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> HTMLResponse | JSONResponse:
    if _wants_json(request):
        return JSONResponse({"ok": False, "error": str(exc.detail)}, status_code=exc.status_code)
    ctx = _error_ctx(request, exc.status_code, str(exc.detail or ""))
    return templates.TemplateResponse("error.html", ctx, status_code=exc.status_code)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> HTMLResponse | JSONResponse:
    logger.error("[500] Unhandled exception on %s %s: %s", request.method, request.url.path, exc, exc_info=True)
    if _wants_json(request):
        return JSONResponse({"ok": False, "error": "internal_server_error"}, status_code=500)
    ctx = _error_ctx(request, 500, "")
    return templates.TemplateResponse("error.html", ctx, status_code=500)
