---
title: "Troubleshooting: common problems"
meta_title: "Troubleshooting — {{app_name}} documentation"
description: "Empty or incomplete crawl, Search Console returning nothing, missing fix buttons, an issue that persists: the causes and what to check."
kind: "Troubleshooting"
section: "Account and billing"
order: 52
updated_at: "2026-10-21"
audience: "All plans"
keywords: ["troubleshooting", "errors", "empty crawl", "problem"]
app_href: "/"
related: ["lancer-un-crawl", "connecter-search-console", "connecter-github", "verifier-une-correction"]
faq:
  - question: "Who should I write to if none of this solves my problem?"
    answer: "To {{support_email}}, including the account email, the project name, the approximate time of the problem and a screenshot. These four items are almost always enough to trace the problem on the server side."
  - question: "Where can I see whether the service itself has an incident?"
    answer: "The Status page shows the state of the services. If a component is in error there, there is no need to look any further on your end."
---

The cases that come up most often, with the first thing to check.

## The crawl only finds the home page

Start by checking the links and the scope actually analyzed.

- Are the links real `<a href="…">` tags? A menu built in JavaScript without
  real links is not followed by anyone.
- Is `robots.txt` blocking crawling? Open `yoursite.com/robots.txt`.
- Do the page limit, the subdomains and the include/exclude filters cover the expected addresses? These choices are in the analysis settings.
- Is the start URL really the canonical version — right protocol, right `www`? Otherwise the
  first hop is a redirect.

## The crawl stopped before the end

Three things to check in the analysis tracking, available from **Activity**:

1. **Page limit reached** — the project's, the plan's, or the monthly quota.
2. **Maximum analysis duration exceeded** — a slow site, or a lot of pages.
3. **The host refused the requests** — 403 or 429. Reduce the simultaneous connections in the crawl options, then review the tracking before running it again.

In the third case, these refusals are **not** counted as errors on your site: they
appear as a crawl incident.

→ [Run a crawl](/docs/lancer-un-crawl)

## Search Console is connected but no data comes in

First check that **the property matches the project URL**.

Check character by character: `https` versus `http`, with `www` versus without, trailing slash.
If you have a Domain property (`sc-domain:…`), use it: it covers every
case.

Other things to check:

- Does the authorized Google account actually have access to this property?
- Is the requested period wide enough? On a new site, 7 days may contain
  nothing.
- Is the minimum impressions threshold excluding all the traffic of a small site?

Reminder: Google publishes its data with a two- to three-day delay.

→ [Connect Search Console](/docs/connecter-search-console)

## The fix buttons do not appear

In the order you should check:

1. **Is a repository linked to this project?** The banner on the Fixes page tells you.
2. **Is GitHub still connected?** A token revoked on the GitHub side cuts the connection without
   warning.
3. **Can the issue be fixed in the code?** A 5xx error or an editorial
   decision has no button, and that is intentional.
4. **Does your plan include AI fixes?** The Free plan does not include them.

→ [Connect GitHub](/docs/connecter-github)

## “Fix created” but nothing in my repository

Look at the pull request link on the task, on the Fixes page. It is almost always
there, on a dedicated branch — and not on your main branch, which is the expected
behavior.

If the PR exists but does not appear in your usual list, check that you are looking at the
right repository and the right target branch.

## The issue is still there after the merge

Three causes, in this order:

1. The deployment did not happen, or it failed.
2. A cache is still serving the old version — CDN, page cache, service worker.
3. The verification crawl predates the deployment.

Quick test: open the URL in a private window and look at the source code. If the fix is
there, it is a cache. If it is not, it is a deployment.

→ [Verify a fix](/docs/verifier-une-correction)

## A score dropped even though I changed nothing

Almost always explainable:

- **New pages were published** and carry template issues.
- **The previous crawl was partial** — canceled, or stopped at a limit. Comparing a full
  crawl to a partial crawl makes no sense.
- **A linked external resource went down**, which makes broken links appear.

Never compare two scores: compare the two crawls. The comparison view names
exactly what appeared.

→ [Read the report](/docs/lire-le-rapport)

## An issue I think is wrong

Open the issue's detail page and look at the evidence column: it shows **exactly** what the
crawler received.

Two common cases where the disagreement is only apparent:

- **You are looking at the rendered page, the crawler read the served HTML.** If a tag is injected
  by JavaScript afterward, it may be missing from the initial response.
- **You are logged in, the crawler is not.** A page that changes depending on the session is not
  the same on both sides.

If the evidence still does not match what you are seeing, write to {{support_email}}
with the URL and the issue in question: that is exactly the kind of case that improves the
crawler.

## Contact support

{{support_email}}, with four items: the account email, the project name, the approximate
time of the problem, and a screenshot. They are almost always enough to trace the problem
on the server side.


## Tracking no longer updates

An interruption message keeps the last state received; it does not prove that the analysis has stopped. Click **Refresh status** before making any new request. If your session has expired, use the sign-in link: it keeps the screen you started from.

A job that finished without a readable report stays flagged as such. An older report available from the site must not be read as the result of the new request.
