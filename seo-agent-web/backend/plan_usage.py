"""Presentation of monthly quotas; enforcement remains in billing and feature guards."""
from __future__ import annotations

from typing import Any

try:
    from . import billing
except ImportError:
    import billing  # type: ignore

MONTHLY_METRICS = (
    ("pages_crawled_month", "Pages analysées", "free"),
    ("assistant_messages_month", "Messages IA", "free"),
    ("ai_corrections_month", "Corrections IA", "free"),
    ("ai_articles_month", "Articles rédigés par l'IA", "pro"),
    ("backlink_searches_month", "Recherches de liens entrants", "solo"),
    ("backlink_replies_month", "Brouillons de messages pour les liens entrants", "solo"),
)


def accessible(plan_key: str, metric: str, catalog: dict[str, Any]) -> bool:
    minimum = next((minimum for name, _label, minimum in MONTHLY_METRICS if name == metric), "free")
    if billing.plan_rank(plan_key) < billing.plan_rank(minimum):
        return False
    if metric == "ai_corrections_month":
        try:
            return int((catalog.get(plan_key, {}).get("correction") or {}).get("max_files") or 0) > 0
        except (TypeError, ValueError):
            return False
    return True


def rows(*, plan_key: str, catalog: dict[str, Any], limits: dict[str, int],
         usage: dict[str, int], is_admin: bool = False) -> list[dict[str, Any]]:
    result = []
    for metric, label, _minimum in MONTHLY_METRICS:
        enabled = accessible(plan_key, metric, catalog)
        limit = limits.get(metric)
        used = usage[metric]
        bounded = isinstance(limit, int) and limit > 0
        state = "admin" if is_admin else "excluded" if not enabled else "finite" if bounded else "unlimited"
        result.append({
            "metric": metric, "label": label, "used": used, "limit": limit if bounded else None,
            "remaining": max(0, limit - used) if state == "finite" else None,
            "percent": max(0, min(100, round(100 * used / limit))) if state == "finite" else 0,
            "bar_value": max(0, min(limit, used)) if state == "finite" else 0,
            "state": state,
        })
    return result
