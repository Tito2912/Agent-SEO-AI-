---
title: "Automate audits and fixes"
meta_title: "Audit automation — {{app_name}} documentation"
description: "Run crawls without having to think about them, fix every error in a single pull request, and track a project's fix tasks."
kind: "Documentation"
section: "Going further"
order: 42
updated_at: "2026-09-30"
audience: "All plans"
keywords: ["automation", "SEO monitoring", "scheduled crawl", "jobs"]
app_href: "/jobs"
related: ["corrections-automatiques", "verifier-une-correction", "lancer-un-crawl"]
faq:
  - question: "How often should you crawl a site?"
    answer: "A stable brochure site: once a month. A blog that publishes every week: every two weeks. An e-commerce site with a changing catalog: every week. And always after a major release, whatever the schedule."
  - question: "Does an automatic crawl use up my page quota?"
    answer: "Yes, exactly like a crawl you start by hand. That is why the page limit per crawl is worth setting before you automate anything."
  - question: "Can I cancel a job that is running?"
    answer: "Yes, from the Jobs page. A canceled crawl keeps what it has already collected, but its report is partial: treat it as a spot check, not as an audit."
---

A one-off audit gives you a snapshot. A routine shows the trends — and above all, it catches
regressions within days of the deployment that introduced them, rather than six
months later.

## The Jobs page

All of the account's jobs, running and past: crawls, fixes, competitor analyses,
opportunity searches.

Each job shows its status, its duration, its logs, and two buttons:

- **Cancel** — stops a running job. What has already been collected is kept.
- **Rerun** — replays the same job with the same parameters. Useful after a network
  outage or when the site was temporarily unavailable.

This is the page to open when something seems stuck. A crawl that has been "running" for three
hours on a 40-page site has a problem, and its logs name it.

## Run crawls regularly

A periodic crawl is what makes the rest of the product useful:

- [fix verification](/docs/verifier-une-correction) needs a crawl run after the deployment
  to confirm anything;
- the comparison between two crawls only exists if there are two crawls;
- a regression introduced by a release is only seen if someone is looking.

The schedules that work, by type of site:

| Site type | Schedule | Why |
| --- | --- | --- |
| Stable brochure site | Monthly | Few changes between two crawls |
| Active blog | Every 2 weeks | Each new post can introduce a template issue |
| E-commerce | Weekly | Changing catalog, removed products, facets |
| Redesign in progress | After each release | That is when regressions happen |

!!! note "The crawl that matters most"
    The one that follows a release. A redesign that adds 40 issues is a
    regression, even if the site looks perfect in the browser.

## Fix in bulk

A project's **Automation** page: the **Fix all in one PR** button.

![Automation screen: connected GitHub repository, bulk fix in a single pull request, past fixes](/static/captures/automatisation.webp "The bulk fix: every fixable issue in a single pull request.")

{{app_name}} takes the latest report, generates the fixes for each fixable
issue, and pushes them all into a single pull request.

In Review mode, the PR waits for you. In Full Access mode, deterministic fixes
are merged automatically, and the others stay open — see
[Connect GitHub](/docs/connecter-github).

The same page shows the project's fix task board, split into four
columns: to do, in progress, done, ignored. Each task comes with its associated pull request and the
result of the post-crawl verification.

Two limits to be aware of: the bulk fix is capped at five runs per
hour, and the number of files a single fix can modify depends on your plan.

## Automate backlink research

Configurable per project, off by default: keywords to watch, sources, daily or weekly
frequency, maximum number of finds per run, and whether drafts are written
automatically.

Leave it off until you have checked by hand that the opportunities it
surfaces are relevant to your site.

→ [Backlinks](/docs/backlinks)

## What automation does not replace

It runs the crawls, prepares the fixes, verifies the results and flags
regressions.

It does not decide which pages deserve your time, does not write your editorial strategy,
and does not merge a pull request whose content comes from a model. Those three remain
decisions for you to make.
