#!/usr/bin/env python3
"""
Fetch Google Search Console Search Analytics data via API and write a CSV.

Auth models:
- Service Account JSON key (server-to-server)
  - Set env `GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json`
    OR pass `--credentials /path/to/service-account.json`
- OAuth "authorized_user" refresh token JSON (end-user OAuth)
  - Provide a JSON containing at least `refresh_token` (and optionally `client_id` / `client_secret`),
    plus set env `GOOGLE_OAUTH_CLIENT_ID` and `GOOGLE_OAUTH_CLIENT_SECRET` if not embedded in the JSON.

Prereqs (Python):
  pip install google-auth requests

Notes:
- The service account email must be added to the GSC property permissions.
- GSC data has a delay; default end date is today-3 days.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests


GSC_SCOPE_READONLY = "https://www.googleapis.com/auth/webmasters.readonly"
GSC_SCOPE_FULL = "https://www.googleapis.com/auth/webmasters"


def _scopes_from_json(raw: Any) -> list[str] | None:
    if not isinstance(raw, dict):
        return None

    scopes_list = raw.get("scopes")
    if isinstance(scopes_list, list):
        out = [str(s).strip() for s in scopes_list if isinstance(s, str) and str(s).strip()]
        return out or None

    scope = raw.get("scope")
    if isinstance(scope, str) and scope.strip():
        out = [s.strip() for s in scope.split() if s.strip()]
        return out or None

    return None


def _today_utc() -> dt.date:
    return dt.datetime.now(dt.timezone.utc).date()


def _parse_date(value: str) -> dt.date:
    return dt.date.fromisoformat(value)


def _get_access_token(credentials_path: Path, *, scopes: list[str] | None = None) -> str:
    try:
        from google.auth.transport.requests import Request  # type: ignore
        from google.oauth2 import service_account  # type: ignore
        from google.oauth2.credentials import Credentials  # type: ignore
    except ModuleNotFoundError as e:
        raise RuntimeError("Missing dependency. Install: pip install google-auth") from e

    raw: Any = None
    try:
        raw = json.loads(credentials_path.read_text(encoding="utf-8"))
    except Exception:
        raw = None

    if isinstance(raw, dict) and str(raw.get("type") or "").strip() == "service_account":
        effective_scopes = scopes or _scopes_from_json(raw) or [GSC_SCOPE_READONLY]
        creds = service_account.Credentials.from_service_account_file(str(credentials_path), scopes=list(effective_scopes))
    else:
        # OAuth "authorized_user" (refresh token) format.
        # Important: avoid sending `scope` on refresh by default.
        # Google will reject refresh requests with mismatched scopes (`invalid_scope`).
        # Using an empty list prevents google-auth from adding the `scope` parameter.
        refresh_scopes: list[str] = []
        refresh_token = str(raw.get("refresh_token") or "").strip() if isinstance(raw, dict) else ""
        client_id = str(raw.get("client_id") or "").strip() if isinstance(raw, dict) else ""
        client_secret = str(raw.get("client_secret") or "").strip() if isinstance(raw, dict) else ""
        token_uri = str(raw.get("token_uri") or "").strip() if isinstance(raw, dict) else ""
        if not client_id:
            client_id = (os.environ.get("GOOGLE_OAUTH_CLIENT_ID") or "").strip()
        if not client_secret:
            client_secret = (os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET") or "").strip()
        if not token_uri:
            token_uri = "https://oauth2.googleapis.com/token"

        if refresh_token and client_id and client_secret:
            creds = Credentials(
                token=None,
                refresh_token=refresh_token,
                token_uri=token_uri,
                client_id=client_id,
                client_secret=client_secret,
                scopes=refresh_scopes,
            )
        else:
            # Fall back to google-auth's built-in file parser for authorized_user JSON.
            # Force empty scopes to avoid `scope=` on refresh.
            creds = Credentials.from_authorized_user_file(str(credentials_path), scopes=refresh_scopes)

    creds.refresh(Request())
    token = getattr(creds, "token", None)
    if not token:
        raise RuntimeError("Unable to obtain access token from credentials")
    return str(token)


def fetch_gsc(
    *,
    credentials_path: Path,
    property_url: str,
    start_date: dt.date,
    end_date: dt.date,
    dimensions: list[str],
    search_type: str,
    row_limit: int,
    timeout_s: float,
) -> list[dict[str, Any]]:
    token = _get_access_token(credentials_path, scopes=None)

    # siteUrl must be URL-encoded when used in path.
    site_encoded = quote(property_url, safe="")
    url = f"https://searchconsole.googleapis.com/webmasters/v3/sites/{site_encoded}/searchAnalytics/query"

    body = {
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
        "dimensions": dimensions,
        "searchType": search_type,
        "rowLimit": int(row_limit),
    }
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    resp = requests.post(url, json=body, headers=headers, timeout=timeout_s)
    if resp.status_code >= 400:
        raise RuntimeError(f"GSC API error: HTTP {resp.status_code} - {resp.text[:300]}")
    data = resp.json()
    rows = data.get("rows", [])
    if not isinstance(rows, list):
        return []
    return [r for r in rows if isinstance(r, dict)]


def inspect_url(
    *,
    credentials_path: Path,
    property_url: str,
    inspection_url: str,
    timeout_s: float,
    language_code: str | None = None,
) -> dict[str, Any]:
    """
    URL Inspection API.

    Docs: https://developers.google.com/webmaster-tools/search-console-api-original/v3/urlInspection.index/inspect
    Endpoint is stable as of v1: POST https://searchconsole.googleapis.com/v1/urlInspection/index:inspect
    """
    # For OAuth refresh tokens, we rely on the stored credential scopes (or token's original scopes)
    # to avoid `invalid_scope` on refresh. Service accounts still require explicit scopes, but that
    # is handled inside `_get_access_token`.
    token = _get_access_token(credentials_path, scopes=[GSC_SCOPE_FULL])
    url = "https://searchconsole.googleapis.com/v1/urlInspection/index:inspect"
    body: dict[str, Any] = {"inspectionUrl": inspection_url, "siteUrl": property_url}
    if isinstance(language_code, str) and language_code.strip():
        body["languageCode"] = language_code.strip()
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    resp = requests.post(url, json=body, headers=headers, timeout=timeout_s)
    if resp.status_code >= 400:
        raise RuntimeError(f"GSC URL Inspection error: HTTP {resp.status_code} - {resp.text[:300]}")
    data = resp.json()
    return data if isinstance(data, dict) else {}


def write_csv(path: Path, rows: list[dict[str, Any]], dimensions: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = [*dimensions, "Clicks", "Impressions", "CTR", "Position"]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            keys = r.get("keys") if isinstance(r.get("keys"), list) else []
            out: dict[str, Any] = {}
            for i, dim in enumerate(dimensions):
                out[dim] = str(keys[i]) if i < len(keys) else ""
            out["Clicks"] = r.get("clicks", 0)
            out["Impressions"] = r.get("impressions", 0)
            out["CTR"] = r.get("ctr", 0)
            out["Position"] = r.get("position", 0)
            w.writerow(out)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch Google Search Console data and write a CSV (service account).")
    p.add_argument("--property", required=True, help="GSC property URL (e.g. https://example.com/ or sc-domain:example.com).")
    p.add_argument("--credentials", help="Path to service account JSON key (default: env GOOGLE_APPLICATION_CREDENTIALS).")
    p.add_argument("--dimensions", default="query", help="Comma-separated dimensions (default: query). Example: query,page")
    p.add_argument("--search-type", default="web", help="web|image|video|news|discover (default: web).")
    p.add_argument("--row-limit", type=int, default=25000, help="Max rows (default: 25000).")
    p.add_argument("--timeout", type=float, default=30.0, help="HTTP timeout seconds (default: 30).")
    p.add_argument("--end-date", help="YYYY-MM-DD (default: today-3 days).")
    p.add_argument("--days", type=int, default=28, help="Number of days in range (default: 28).")
    p.add_argument("--output", required=True, help="Output CSV path.")
    return p.parse_args(argv)


def main(argv: list[str]) -> int:
    args = _parse_args(argv)

    credentials = args.credentials or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials:
        print("[ERROR] Missing credentials. Set GOOGLE_APPLICATION_CREDENTIALS or pass --credentials.", file=sys.stderr)
        return 2

    credentials_path = Path(credentials).expanduser().resolve()
    if not credentials_path.exists():
        print(f"[ERROR] Credentials file not found: {credentials_path}", file=sys.stderr)
        return 2

    end_date = _parse_date(args.end_date) if args.end_date else (_today_utc() - dt.timedelta(days=3))
    days = max(1, int(args.days))
    start_date = end_date - dt.timedelta(days=days - 1)

    dimensions = [d.strip() for d in str(args.dimensions).split(",") if d.strip()]
    if not dimensions:
        dimensions = ["query"]

    try:
        rows = fetch_gsc(
            credentials_path=credentials_path,
            property_url=str(args.property),
            start_date=start_date,
            end_date=end_date,
            dimensions=dimensions,
            search_type=str(args.search_type),
            row_limit=int(args.row_limit),
            timeout_s=float(args.timeout),
        )
        out_path = Path(args.output).expanduser().resolve()
        write_csv(out_path, rows, dimensions=dimensions)
        print(f"[OK] Wrote {out_path}")
        return 0
    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
