"""Read finalized, exact-page Search Analytics periods with existing authorization.

Only a fixed Google endpoint is contacted. No new scope, property discovery,
redirect, pagination, retry, or inference from the query-level top rows.
"""
from __future__ import annotations

import json
import math
from calendar import monthrange
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit
from zoneinfo import ZoneInfo

import requests


MAX_RESPONSE_BYTES = 65536
TIMEOUT = (5, 15)
FINAL_DATA_LAG_DAYS = 3
SEARCH_TYPES = frozenset({"web", "image", "video", "news", "discover", "googleNews", "webMultimodal"})


def _today_pacific() -> date:
    return datetime.now(ZoneInfo("America/Los_Angeles")).date()


def _oldest_supported_date(today: date) -> date:
    year, month = divmod(today.year * 12 + today.month - 1 - 16, 12)
    # Keep one conservative day inside the rolling sixteen-month boundary;
    # absent data before retention must not be reported as zero traffic.
    return date(year, month + 1, min(today.day, monthrange(year, month + 1)[1])) + timedelta(days=1)


def _date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(value) if isinstance(value, str) and len(value) == 10 else None
    except ValueError:
        return None


def _url(value: Any):
    if not isinstance(value, str) or not value or len(value) > 4096 or any(c.isspace() or c == "\\" or ord(c) < 32 for c in value):
        return None
    try:
        parsed = urlsplit(value)
        if parsed.scheme not in {"https", "http"} or not parsed.hostname or parsed.username or parsed.password or parsed.fragment:
            return None
        _ = parsed.port
        return parsed
    except ValueError:
        return None


def _valid_scope(property_url: Any, page_url: Any) -> bool:
    page = _url(page_url)
    if page is None or not isinstance(property_url, str):
        return False
    if property_url.startswith("sc-domain:"):
        domain = property_url[len("sc-domain:"):]
        try:
            ascii_domain = domain.encode("idna").decode("ascii").lower()
        except UnicodeError:
            return False
        if not ascii_domain or any(c not in "abcdefghijklmnopqrstuvwxyz0123456789-." for c in ascii_domain):
            return False
        if any(not part or part.startswith("-") or part.endswith("-") for part in ascii_domain.split(".")):
            return False
        host = page.hostname.encode("idna").decode("ascii").lower()
        return host == ascii_domain or host.endswith("." + ascii_domain)
    prop = _url(property_url)
    if prop is None or prop.query:
        return False
    def origin(parts):
        return parts.scheme, parts.hostname.lower(), parts.port or (443 if parts.scheme == "https" else 80)
    if origin(prop) != origin(page):
        return False
    # A URL-prefix property is case-sensitive and includes its path prefix.
    return (page.path or "/").startswith(prop.path or "/")


def _metric(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) and parsed >= 0 else None


def _read_json(response):
    """Bound decompressed bytes too; never read or return response.text."""
    raw = bytearray()
    for chunk in response.iter_content(chunk_size=8192):
        if not isinstance(chunk, bytes):
            raise ValueError("invalid_response")
        if len(raw) + len(chunk) > MAX_RESPONSE_BYTES:
            raise ValueError("response_too_large")
        raw.extend(chunk)
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeError):
        raise ValueError("invalid_json") from None


def fetch_page_period(*, fetch_module, credentials_path, property_url: str, page_url: str,
                      start_date, end_date, search_type: str = "web", request_post=None) -> dict[str, Any]:
    """Return the agent_results period contract, including unavailable states.

    ``request_post`` is an injected requests-compatible callable. It must support
    stream=True and return status_code/iter_content()/close(). No response body or
    exception text is exposed. A valid empty response establishes zero reported
    rows only for this exact page/property/type/finalized date window.
    """
    start, end = _date(start_date), _date(end_date)
    output = {"ok": False, "complete": False, "reason": "invalid_scope",
              "page_url": page_url if _url(page_url) else "", "property": "",
              "start_date": start.isoformat() if start else "", "end_date": end.isoformat() if end else "",
              "dimension": "page", "filters": {"search_type": search_type
              if isinstance(search_type, str) and search_type in SEARCH_TYPES else ""},
              "rows": [], "data_state": "final", "calendar_timezone": "America/Los_Angeles"}
    try:
        valid_scope = _valid_scope(property_url, page_url)
    except (ValueError, UnicodeError):
        valid_scope = False
    if not valid_scope:
        return output
    output["property"] = property_url
    if not isinstance(search_type, str) or search_type not in SEARCH_TYPES:
        return {**output, "reason": "invalid_search_type"}
    if not start or not end or end < start:
        return {**output, "reason": "invalid_dates"}
    today = _today_pacific()
    if end > today - timedelta(days=FINAL_DATA_LAG_DAYS):
        return {**output, "reason": "period_not_final"}
    if start < _oldest_supported_date(today):
        return {**output, "reason": "period_out_of_retention"}
    try:
        token = fetch_module._get_access_token(Path(credentials_path), scopes=None)
        if not isinstance(token, str) or not token or any(c.isspace() or ord(c) < 32 for c in token):
            raise ValueError()
    except Exception:
        return {**output, "reason": "authorization_unavailable"}
    endpoint = ("https://searchconsole.googleapis.com/webmasters/v3/sites/"
                + quote(property_url, safe="") + "/searchAnalytics/query")
    payload = {"startDate": start.isoformat(), "endDate": end.isoformat(), "dimensions": ["page"],
               "dimensionFilterGroups": [{"groupType": "and", "filters": [
                   {"dimension": "page", "operator": "equals", "expression": page_url}]}],
               "type": search_type, "dataState": "final", "aggregationType": "auto", "rowLimit": 2}
    post = request_post if request_post is not None else requests.post
    response = None
    try:
        response = post(endpoint, json=payload, headers={"Authorization": "Bearer " + token, "Accept": "application/json"},
                        timeout=TIMEOUT, allow_redirects=False, stream=True)
        status = response.status_code
        if status != 200:
            reason = {401: "authorization_refused", 403: "property_access_refused", 429: "rate_limited"}.get(status, "http_error")
            if 300 <= status < 400:
                reason = "redirect_refused"
            return {**output, "reason": reason}
        try:
            data = _read_json(response)
        except ValueError as exc:
            return {**output, "reason": str(exc) if str(exc) in {"response_too_large", "invalid_json", "invalid_response"} else "invalid_response"}
    except Exception:
        return {**output, "reason": "network_error"}
    finally:
        if response is not None:
            try:
                response.close()
            except Exception:
                pass
    if not isinstance(data, dict) or "error" in data:
        return {**output, "reason": "invalid_response"}
    aggregation = data.get("responseAggregationType")
    if not isinstance(aggregation, str) or aggregation not in {"auto", "byPage"}:
        return {**output, "reason": "aggregation_mismatch"}
    metadata = data.get("metadata")
    if metadata is not None:
        if not isinstance(metadata, dict):
            return {**output, "reason": "invalid_response"}
        if metadata.get("first_incomplete_date") or metadata.get("first_incomplete_hour"):
            return {**output, "reason": "period_not_final"}
    rows = data.get("rows", [])
    if not isinstance(rows, list) or len(rows) > 1:
        return {**output, "reason": "unexpected_rows"}
    normalized = []
    if rows:
        row = rows[0]
        if not isinstance(row, dict) or row.get("keys") != [page_url]:
            return {**output, "reason": "page_mismatch"}
        clicks, impressions, ctr, position = (_metric(row.get(k)) for k in ("clicks", "impressions", "ctr", "position"))
        if (clicks is None or impressions is None or clicks != int(clicks) or impressions != int(impressions)
                or clicks > impressions or ctr is None or ctr > 1 or position is None
                or (impressions > 0 and position <= 0)):
            return {**output, "reason": "invalid_metrics"}
        actual_ctr = clicks / impressions if impressions else 0.0
        if not math.isclose(ctr, actual_ctr, rel_tol=1e-6, abs_tol=1e-8):
            return {**output, "reason": "invalid_metrics"}
        normalized.append({"page": page_url, "clicks": int(clicks), "impressions": int(impressions),
                           "ctr": actual_ctr, "position": position if impressions else None})
    return {**output, "ok": True, "complete": True, "reason": "", "rows": normalized}
