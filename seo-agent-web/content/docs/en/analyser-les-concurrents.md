---
title: "Analyze competitors"
meta_title: "Competitor analysis — {{app_name}} documentation"
description: "Crawl a rival site, compare the topics it covers with yours, and read the uncovered topics without getting the interpretation wrong."
kind: "Documentation"
section: "Going further"
order: 40
updated_at: "2026-10-19"
audience: "Pro and Business"
keywords: ["competitors", "competitive analysis", "uncovered topics", "content gap"]
app_href: "/"
related: ["opportunites-de-mots-cles", "backlinks", "plans-et-quotas"]
faq:
  - question: "Why only 100 pages per competitor?"
    answer: "Because this crawl explores a site that isn't yours. A hundred pages are enough to map a site's topics, and the limit ensures the analysis remains polite sampling rather than scraping."
  - question: "Does {{app_name}} fix my site based on competitors?"
    answer: "No. Uncovered topics are reported, never turned into an automatic fix. Deciding to write a page is an editorial choice, and no one should delegate it to a machine."
  - question: "How often are competitors re-analyzed?"
    answer: "Automatically once a month — a rival site doesn't republish every week. You can trigger an on-demand analysis from the page when you know it has changed."
---

*Available from the Pro plan.*

The question this page answers: **what do your competitors talk about that you don't
cover?**

Not “what's their score”, nor “how many backlinks do they have”. What they talk about — because
it's the only competitive information that turns directly into work.

## Add a competitor

A project's **Competitors** page, URL field. Up to **5 competitors** per project.

Each addition triggers a crawl of the rival site, capped at **100 pages**, at a reduced pace.
This limit isn't a technical constraint: it's the difference between sampling a
site and scraping it. A hundred pages are enough to map a site's topics; beyond that, it
just repeats itself.

The analysis runs in the background; the page shows the status of each competitor: pending,
in progress, ready, or failed with the reason.

## Read the comparison

The product extracts the topics of each page — from its URL, its `title`, its headings — on
both sides, then matches them up.

![Competitors screen: tracked sites, then topics covered by competitors with the overlap rate](/static/captures/concurrents.webp "Your competitors' topics, compared with your own pages.")

You get two families of results, and **uncovered topics are shown
first**.

### Uncovered topics

A theme the competitor covers and that nothing on your site addresses.

It's the interesting find, and it's also the one the product won't act on:
writing a page is an editorial decision. {{app_name}} flags it and stops there.

Read it with some perspective. A competitor may publish on a topic because it's their business and
not yours, or simply because they got it wrong. A gap is not always a shortcoming.

### Topics covered on both sides

You have a page on the topic, and so do they. The comparison shows yours.

This is where the work pays off most: the page exists, it's indexed, it has a
history. Strengthening it costs infinitely less than creating a new one.

Cross-check with [keyword opportunities](/docs/opportunites-de-mots-cles): if this page
ranks in position 12 for the topic's query, you have both the what and the why.

For a topic not found in the analyzed pages, “Prepare an article” opens Content with the topic pre-filled, without starting any writing. The “Content only” mode works without GitHub. For a page that already exists, review it in your platform or prepare a GitHub improvement if a repository is connected.

## The four states of the page

The page explicitly distinguishes four situations, rather than showing an empty table in
three of them:

1. **No access** — your plan doesn't include the feature.
2. **No competitor added** — you need to start by adding one.
3. **Competitor added, never analyzed** — the crawl hasn't run yet.
4. **Comparison available** — the only case where a table makes sense.

This isn't an interface detail: these four states call for four different actions, and
an empty table would lump them all together.

## Refresh

Competitors are re-analyzed automatically **once a month**. A rival site doesn't
republish every week, and a monthly crawl is enough to see its new
sections appear.

You can run an analysis again on demand from the page — when you know a
competitor has just redesigned its site, for example.

## What this page doesn't do

- **It doesn't estimate their traffic.** No real data from their Search Console is
  accessible, and an estimate presented as a fact would be misleading.
- **It doesn't analyze their backlinks.** That's the job of the [Backlinks](/docs/backlinks) page,
  with your own data sources.
- **It doesn't trigger any fix.** Nothing that comes from a third-party site goes into the
  automatic fixer.
