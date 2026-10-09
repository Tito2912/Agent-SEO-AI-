"""Direct, coherent 404/410 observations, not every response in the 4xx family."""

from urllib.parse import urlsplit

TERMINAL = {404, 410}


def verified_urls(urls: list[str], pages: list[dict] | None, identify) -> tuple[dict[str, int], list[str]]:
    requested = {}
    for row in pages or []:
        if isinstance(row, dict):
            requested.setdefault(identify(row.get("url")), []).append(row)
    accepted, refused = {}, []
    for value in urls:
        source = identify(value)
        rows = requested.get(source, [])
        codes = {row.get("status_code") for row in rows if type(row.get("status_code")) is int}
        if (source and not urlsplit(source).username and not urlsplit(source).password and rows and len(codes) == 1
                and codes <= TERMINAL and all(type(row.get("status_code")) is int and not row.get("error")
                    and not row.get("blocked_by_host") and identify(row.get("final_url") or row.get("url")) == source
                    and not row.get("redirect_chain") and not row.get("redirect_statuses")
                    and str(row.get("content_type") or "").split(";", 1)[0].strip().lower() in {"text/html", "application/xhtml+xml"}
                    for row in rows)):
            accepted[source] = codes.pop()
        else:
            refused.append(source or "URL inconnue")
    return accepted, sorted(set(refused))
