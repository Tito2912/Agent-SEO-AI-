---
title: "Backlinks: inventory, opportunities and monitoring"
meta_title: "Backlinks and link building — {{app_name}} documentation"
description: "Import your link profile, surface backlink opportunities, prepare outreach messages and detect the links you have lost."
kind: "Documentation"
section: "Going further"
order: 41
updated_at: "2026-10-20"
audience: "Solo, Pro and Business for opportunities"
keywords: ["backlinks", "link building", "inbound links", "Ahrefs"]
app_href: "/"
related: ["analyser-les-concurrents", "automatiser-les-audits", "plans-et-quotas"]
faq:
  - question: "Does {{app_name}} build backlinks for me?"
    answer: "No, and no one honestly can. The product finds places where your site would belong, prepares a message, and lets you send it. The decision to publish always belongs to someone else."
  - question: "Do I need an Ahrefs subscription?"
    answer: "No. Ahrefs sync is an option for those who have one. Without it, you import a CSV from any tool, or you work only on opportunities."
  - question: "What happens if a link I earned disappears?"
    answer: "A periodic check visits the pages of links marked “Won” or “Contacted” and flags those that no longer point to you. It's the part of link building everyone forgets."
---

Link building is the field where tools promise the most and deliver the least. What
{{app_name}} does here is deliberately limited: it takes inventory, it spots, it prepares, it
monitors. It doesn't get links for you, because no one can.

## Take inventory of your backlinks

A project's **Backlinks** page. The “Links to your site” block shows the imported sources; the “Links in the analyzed pages” block shows the internal and outgoing links from the selected analysis. Two ways to fill it:

![Link building block from the crawl: analyzed pages, orphan pages and external domains](/static/captures/netlinking.webp "What the crawl already tells you about your links: analyzed pages, orphan pages, external domains.")

**CSV import** — from any tool that exports a link profile. It's the universal
route, and it requires no subscription.

**Ahrefs sync** — if you have access to the Ahrefs API. Four possible
scopes: domain, subdomains, URL prefix, or exact URL. The volume retrieved is
capped to avoid pulling in an entire profile at once.

Imports show the referring domains, target pages and anchors recorded by each source; they are not an exhaustive inventory. The **orphan pages** in the analysis block are those with no incoming internal link found in the analyzed pages. This figure does not measure the absence of links from other sites.

## Opportunities

*Available from the Solo plan.*

The **Promote my site** page. {{app_name}} looks for places where your site would legitimately
belong: discussions where your topic comes up, resource pages, lists, articles that mention
comparable tools.

The search relies on keywords you define, and on sources you
choose.

Each opportunity has a status that you update by hand:

| Status | Meaning |
| --- | --- |
| **New** | Detected, not handled yet |
| **Contacted** | You sent a message |
| **Won** | The link exists |
| **Lost** | Declined, or link removed |

This isn't decorative: it's what keeps you from contacting the same site three times, and what
feeds the monitoring described below.

## Preparing messages

For a given opportunity, {{app_name}} can write a draft outreach message:
context of the target page, why your content belongs there, a tone suited to the
platform.

Three things to know, and they are deliberate:

- **It's a draft.** It's meant to be reviewed and edited. A generic message sent
  as is gets spotted a mile away and deleted just as fast.
- **You send it yourself.** “Copy and open” prepares the text and opens the discussion. After posting on the site concerned, come back and click “I posted the reply”. This declaration does not yet prove the link exists; it updates your tracking.
- **Drafts are counted** in a monthly quota, like searches. Both
  depend on your plan.

!!! warning "The line not to cross"
    Mass-produced automated link building — comments, forums, profiles — hurts a site more
    than it helps, and search engines have long known how to recognize it. This
    module's quotas are low on purpose: they push you toward a few relevant contacts rather than
    three hundred messages.

## Automation, if you want it

A periodic opportunity search can be enabled per project: keywords to monitor,
sources, daily or weekly frequency, maximum number of finds per run, and
whether drafts are written automatically. This automation does not post messages on third-party sites; sending remains manual.

It is **disabled by default**, and that's the right setting until you have checked
by hand that the opportunities surfaced are relevant to your site.

## Monitoring earned links

This is the part most tools forget. An earned link is never guaranteed: the page
gets redesigned, the article is unpublished, the link switches to `nofollow`, or it disappears
without explanation.

{{app_name}} periodically revisits the pages of opportunities marked **Won** or
**Contacted**, and flags those that no longer point to you.

A lost link can often be recovered with a polite message — much more easily than
getting a new one.

## How it compares with the rest

Link building is slow and uncertain. Before spending a budget on it, check that the pages
you want to push are **indexable**, properly titled and linked to each other:
an external link to a `noindex` page is money down the drain.

→ [Issues and priorities](/docs/anomalies-et-priorites)
