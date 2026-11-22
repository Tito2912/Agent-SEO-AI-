"""Compare dated, explicitly complete page-level GSC measurements without causality.

No network, persistence or inferred commercial outcomes. The caller supplies an
explicit page, property and filters for each period and is responsible for obtaining
the data with the customer's authorized connection.
"""
from __future__ import annotations

import json
import math
from datetime import date, datetime
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo


def _page(value: Any) -> str:
    if not isinstance(value, str) or any(c.isspace() or ord(c) < 32 or c == "\\" for c in value):
        return ""
    try:
        p = urlsplit(value)
        if p.scheme not in {"http", "https"} or not p.hostname or p.username or p.password:
            return ""
        _ = p.port
        return urlunsplit((p.scheme, p.netloc.lower(), p.path or "/", p.query, ""))
    except ValueError:
        return ""


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) and parsed >= 0 else None


def _date(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def summarise_period(period: dict | None, *, page_url: str) -> dict[str, Any]:
    """Normalize a complete, page-scoped API period; absent data stays absent.

    Required: ok=True, complete=True, page_url, property, start_date/end_date,
    filters={}, dimension='page', rows=[{page,clicks,impressions,position,...}].
    Explicit empty rows on a complete response mean zero clicks/impressions.
    Unknown position on a nonempty row does not erase measured clicks/impressions.
    Native page keys=[page] are accepted as well as normalized page or keyword.
    Query-grouped results are refused because they omit anonymized queries and can
    be truncated; a separate page aggregate must be requested by the caller.
    """
    out = {"status": "unavailable", "reason": "missing_data", "metrics": None}
    if not isinstance(period, dict):
        return out
    target = _page(page_url)
    if not target or _page(period.get("page_url")) != target:
        return {**out, "status": "invalid", "reason": "page_mismatch"}
    start = _date(period.get("start_date") or period.get("date_start"))
    end = _date(period.get("end_date") or period.get("date_end"))
    if not start or not end or end < start:
        return {**out, "status": "invalid", "reason": "invalid_dates"}
    if period.get("ok") is not True:
        return {**out, "reason": "incomplete_data"}
    complete = period.get("complete") is True
    if period.get("dimension") != "page" or not isinstance(period.get("property"), str) or not period["property"].strip():
        return {**out, "status": "invalid", "reason": "invalid_scope"}
    filters = period.get("filters")
    if not isinstance(filters, dict):
        return {**out, "status": "invalid", "reason": "invalid_filters"}
    try:
        json.dumps(filters, sort_keys=True, allow_nan=False)
    except (ValueError, TypeError):
        return {**out, "status": "invalid", "reason": "invalid_filters"}
    rows = period.get("rows")
    if not isinstance(rows, list):
        return {**out, "reason": "missing_rows"}
    if not rows and not complete:
        return {**out, "reason": "incomplete_data"}
    clicks = impressions = position_sum = position_weight = 0.0
    position_complete = True
    seen = set()
    for row in rows:
        if not isinstance(row, dict):
            return {**out, "status": "invalid", "reason": "invalid_row"}
        keys = row.get("keys") if isinstance(row.get("keys"), list) else []
        url = _page(row.get("page") or row.get("url") or row.get("keyword") or (keys[0] if len(keys) == 1 else ""))
        if url != target or row.get("query") or len(keys) > 1:
            return {**out, "status": "invalid", "reason": "row_scope_mismatch"}
        # Multiple page aggregates cannot be added without disjoint daily buckets.
        day = _date(row.get("date")) if row.get("date") else None
        identity = day.isoformat() if day else "aggregate"
        if identity in seen or (day and not start <= day <= end) or (row.get("date") and day is None):
            return {**out, "status": "invalid", "reason": "duplicate_or_invalid_bucket"}
        if seen and (identity == "aggregate" or "aggregate" in seen):
            return {**out, "status": "invalid", "reason": "overlapping_buckets"}
        seen.add(identity)
        c, i, p = (_number(row.get(k)) for k in ("clicks", "impressions", "position"))
        if c is None or i is None or c > i or c != int(c) or i != int(i):
            return {**out, "status": "invalid", "reason": "invalid_counts"}
        clicks += c
        impressions += i
        if i and (p is None or p <= 0):
            position_complete = False
        elif i:
            position_sum += p * i
            position_weight += i
    metrics = {"clicks": int(clicks), "impressions": int(impressions),
               "ctr": clicks / impressions if impressions else None,
               "position": position_sum / position_weight if position_complete and position_weight else None}
    return {"status": "complete" if complete else "partial", "reason": "" if complete else "incomplete_data",
            "metrics": metrics,
            "page_url": target, "property": period["property"], "filters": filters,
            "start_date": start.isoformat(), "end_date": end.isoformat(),
            "days": (end - start).days + 1, "dimension": "page"}


def compare_periods(before: dict | None, after: dict | None, *, page_url: str,
                    change_at: str | None = None) -> dict[str, Any]:
    """Comparable observations remain observations, never an attributed SEO gain."""
    left, right = (summarise_period(p, page_url=page_url) for p in (before, after))
    result = {"comparable": False, "reason": "missing_data", "before": left, "after": right,
              "changes": None, "causality": "not_established", "commercial_results": None,
              "notes": ["L’évolution observée ne prouve pas l’effet de la modification."]}
    if left["status"] != "complete" or right["status"] != "complete":
        result["reason"] = "incomplete_or_invalid_period"
        return result
    if any(left[key] != right[key] for key in ("page_url", "property", "filters", "dimension")):
        result["reason"] = "scope_mismatch"
        return result
    if left["days"] != right["days"]:
        result["reason"] = "duration_mismatch"
        return result
    if left["end_date"] >= right["start_date"]:
        result["reason"] = "overlapping_or_reversed_periods"
        return result
    if change_at is not None:
        try:
            changed = datetime.fromisoformat(str(change_at).replace("Z", "+00:00"))
            if changed.tzinfo is None:
                raise ValueError()
            # Search Analytics dates are Pacific dates, not the operator's local
            # calendar. Exclude the entire change day in that same calendar.
            change_date = changed.astimezone(ZoneInfo("America/Los_Angeles")).date().isoformat()
        except (ValueError, TypeError):
            result["reason"] = "invalid_change_date"
            return result
        if not left["end_date"] < change_date < right["start_date"]:
            result["reason"] = "periods_do_not_bracket_change"
            return result
    changes = {}
    for metric in ("clicks", "impressions", "ctr", "position"):
        a, b = left["metrics"][metric], right["metrics"][metric]
        changes[metric] = {"absolute": b - a if a is not None and b is not None else None,
                           "relative": (b - a) / a if a and b is not None else None}
    result.update(comparable=True, reason="", changes=changes)
    if left["days"] % 7:
        result["notes"].append("Les périodes ne couvrent pas un nombre entier de semaines : les jours de semaine peuvent différer.")
    result["notes"].append("Saisonnalité, autres modifications et mises à jour des moteurs peuvent contribuer à l’évolution.")
    return result
