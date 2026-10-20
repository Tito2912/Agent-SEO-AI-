---
slug: "frequence-crawl-seo-site-vitrine-ecommerce-blog"
title: "How often should you crawl a brochure site, an online store or a blog?"
meta_title: "SEO crawl frequency by type of website"
description: "The right audit cadence depends on how fast a site can break, not on its size. Benchmarks by site type, plus the triggers you can't afford to miss."
kind: "Guide"
updated_at: "2026-09-30"
published_at: "2026-05-12"
audience: "Freelancers, agencies and in-house teams"
keywords: ["crawl frequency", "SEO monitoring", "recurring SEO audit"]
related: ["audit-seo-technique-checklist-priorites", "regression-seo-apres-mise-en-production"]
cta: "{{app_name}} runs the crawl, the comparison and the fix verification back to back: you don't rerun anything by hand, and a regression shows up within days of the deployment that introduced it."
faq:
  - question: "Can crawling too often hurt my site?"
    answer: "A polite crawl — robots.txt respected, crawl-delay honored, concurrent requests limited — is painless. What strains a server is an aggressive crawl, not a frequent one."
  - question: "Do I need to crawl the entire site every time?"
    answer: "To compare two crawls, yes: comparing a full crawl with a partial one produces phantom changes. A crawl limited to one section is useful for verifying a specific fix, not for tracking a trend."
---

That's the wrong question. It's not the size of the site that sets the cadence, it's **how
fast it can break**.

A 50,000-page catalog that never changes needs less monitoring than a 30-page brochure
site in the middle of a redesign.

## Benchmarks by site type

| Type | Cadence | What breaks, and how fast |
| --- | --- | --- |
| **Stable brochure site** | Monthly | Almost nothing between two crawls. The monthly crawl is there to catch what comes from outside: dead outbound links, the SSL certificate, redirects set by the host. |
| **Active blog** | Every 2 weeks | Every post goes through the same template. One template change introduces the issue across the entire archive at once. |
| **E-commerce** | Weekly | Discontinued products that turn into 404s that are still linked, facets that generate URLs, stock changes that drop pages out of the internal linking. |
| **Site being redesigned** | After every release | That's where 90% of SEO regressions are born. |
| **Multilingual site** | Monthly, full crawl | `hreflang` relationships can only be verified if the crawl sees both sides at the same time. |

## Triggers that override the calendar

Whatever your cadence, these five events call for an immediate crawl:

1. **A production release.** Especially if it touches the templates, the routing or the
   hosting configuration.
2. **A change of CMS, theme or host.** Staging `robots.txt` files that make it to
   production are a textbook case.
3. **A URL migration.** The control crawl is part of the migration, not of the aftermath.
4. **A drop in impressions in Search Console.** The crawl tells you whether the cause is technical.
5. **A batch of fixes deployed.** Without a follow-up crawl, you have no proof that it
   worked.

The fifth is the most neglected, and it's the one that costs the most: an unverified
fix is a fix you know nothing about.

## What a regular cadence gives you

A single crawl gives you a snapshot. A series gives you three things no snapshot contains:

![Bar chart of a site's health score, one bar per crawl](/static/captures/evolution-du-score.webp "A regular history: one bar per crawl.")

**The direction of travel.** Is the number of issues going down or up? On a living site,
stable means you're fixing things as fast as you're breaking them.

**When a problem first appeared.** An issue that showed up between the crawl on the 3rd and the one on the 17th
can be tied to a specific deployment. Without a history, you're digging through six months of commits.

**Proof of the work done.** If you bill for this audit, the comparison between two crawls is
your most convincing deliverable — far more than a score.

## The partial crawl trap

For the comparison to mean anything, the crawls have to be comparable. A crawl stopped
halfway — page limit reached, canceled, timed out — produces changes that
don't exist: 200 “missing” pages that simply weren't visited.

Two precautions:

- set the page limit **above** the site's actual size, with headroom for
  growth;
- check that the crawl finished normally before drawing any conclusion from a
  comparison.

## A sustainable cadence beats an ideal one

A monthly crawl that actually runs every month is infinitely better than a weekly crawl
abandoned after three weeks.

Start at the cadence you'll stick to, add the event-based triggers, and increase the
frequency only if the crawls regularly turn something up. If three months of weekly crawls
surface nothing new, you're crawling too often.
