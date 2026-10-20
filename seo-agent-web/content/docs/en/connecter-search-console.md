---
title: "Connect Google Search Console"
meta_title: "Connect Search Console — {{app_name}} documentation"
description: "Link Search Console to a {{app_name}} project: Google authorization, choosing the property, period, URL inspection and the Bing connection."
kind: "Documentation"
section: "Search Console and keywords"
order: 30
updated_at: "2026-10-20"
audience: "All plans"
keywords: ["Google Search Console", "GSC", "impressions", "URL inspection"]
app_href: "/settings/accounts#gsc-oauth-card"
related: ["opportunites-de-mots-cles", "suivre-la-performance", "anomalies-et-priorites"]
faq:
  - question: "Which property should I choose: domain or URL prefix?"
    answer: "The Domain property, when one exists: it covers all subdomains and both protocols. A URL-prefix property must match the project's start URL exactly, protocol and www included."
  - question: "Why is my Performance page empty when the connection is green?"
    answer: "Almost always a mismatch between the Search Console property and the project URL — https versus http, with www versus without. The page states the exact reason instead of showing an empty table."
  - question: "Can {{app_name}} change anything in my Search Console?"
    answer: "No. The access requested is read-only. The product reads performance data and, if you enable it, queries the URL Inspection API — which is also read-only."
  - question: "Is Search Console data real-time?"
    answer: "No, Google publishes with a two- to three-day delay. A fix deployed yesterday won't show up today, and a drop you notice today started the day before yesterday."
---

Without Search Console, {{app_name}} knows your site. With it, it also knows what Google
does with it — and it's this second piece of information that makes the priorities credible.

A missing `meta description` on a page with 12,000 impressions and the same one on an archive
with zero impressions stop being the same problem.

## Authorize Google

**My account → Connections → Google Search Console → Connect**. You are redirected to Google,
you choose the account that has access to the property, and you come back.

The authorization is **read-only**. {{app_name}} cannot modify, submit or
delete anything in your Search Console.

## Choose the property

The link is made project by project, in the analysis settings, in the **GSC
property** field.

This is where almost every connection fails, for a single reason: the
property does not match the project URL.

| Property type | Format | Covers |
| --- | --- | --- |
| Domain | `sc-domain:example.com` | All subdomains, http and https |
| URL prefix | `https://www.example.com/` | Exactly that prefix, nothing else |

Choose the **Domain** property if it exists: it avoids the whole problem. Otherwise, check
character by character that the prefix matches your project's start URL — `https`
versus `http`, with `www` versus without, trailing slash included.

If no data comes through, the page tells you *why* instead of showing an empty
table. Read that message: it names the cause in nearly every case.

## Settings

In the project's analysis settings, Search Console section:

- **Enable** — joins search data to the crawl.
- **Period** — the number of days of history to fetch. 28 days by default, which smooths out
  weekly variations without drowning out a recent change.
- **Search type** — web, image, video, news. Leave it on “web” unless your traffic
  really comes from elsewhere.
- **Minimum impressions** — the threshold below which a query is ignored. It removes the long
  tail at two impressions, where click-through rate means nothing statistically.

## URL inspection

A separate option, slower, and sometimes decisive: {{app_name}} queries Google's URL
Inspection API for a sample of pages.

What it provides that the crawl cannot: **Google's own verdict**. Whether the page is
indexed, the canonical Google selected — which is not always the one you declare —
the date of the last crawl, and presence in a sitemap Google knows about.

Three settings: enable, maximum number of URLs to inspect, and timeout. The API is
rate-limited by Google, hence the sample.

!!! note "When it's worth it"
    When the crawl says a page is perfectly indexable and yet it receives
    no impressions. Inspection settles it: either Google doesn't know the page, or it selected
    a different canonical, or it saw the page and set it aside.

## Bing Webmaster Tools

Same principle, separate connection, and the same settings: period, minimum impressions,
queries and pages. Bing also exposes its own crawl issues, its known sitemaps and
its blocked URLs.

The volume is lower than Google's almost everywhere, but the data is free and
sometimes more explicit about crawl blocks.

## What it unlocks

Once connected, Search Console feeds three things:

1. **Issue prioritization** — by real impressions, no longer just by severity.
2. **The Performance page** — clicks, impressions, position, page by page and query by
   query.
3. **Keyword opportunities**, the feature that makes this connection most
   worthwhile.

→ [Keyword opportunities](/docs/opportunites-de-mots-cles)
