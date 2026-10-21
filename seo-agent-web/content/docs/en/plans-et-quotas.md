---
title: "Plans and quotas"
meta_title: "Plans, limits and quotas — {{app_name}} documentation"
description: "What each {{app_name}} plan includes: projects, crawled pages, AI fixes, Core Web Vitals, and how each quota is actually counted."
kind: "Reference"
section: "Account and billing"
order: 50
updated_at: "2026-10-21"
audience: "All plans"
keywords: ["pricing", "quotas", "limits", "subscription"]
app_href: "/billing"
related: ["corrections-automatiques", "lancer-un-crawl", "rediger-du-contenu", "comptes-d-equipe"]
faq:
  - question: "How is an AI fix counted?"
    answer: "One file written by the model equals one unit — on WordPress, one title or meta description written for a page. A pull request that modifies a single template and fixes 300 URLs therefore costs one unit: the quota measures the model's work, not the number of rows in the report."
  - question: "What happens when I reach a quota?"
    answer: "The action in question is refused with a message that names the quota reached, and a link to the Subscription page. Nothing else stops: an exhausted fixes quota does not block crawls, reports or exports."
  - question: "Do quotas carry over from one month to the next?"
    answer: "No. Monthly counters reset on the first day of each month at 00:00 UTC, independently of your subscription renewal date."
  - question: "Can I change plans in the middle of the month?"
    answer: "Yes, from the Subscription page. An upgrade takes effect immediately; a downgrade is scheduled for the end of the current period and remains visible until then."
---

The figures on this page are the ones the product actually applies, not a copy of the
price list: they are read from the configuration in effect at the moment you view this
page.

## The table

| | Free | Solo | Pro | Business |
| --- | --- | --- | --- | --- |
| **Price** | {{price_free}} | {{price_solo}} | {{price_pro}} | {{price_business}} |
| **Projects** | {{projects_free}} | {{projects_solo}} | {{projects_pro}} | {{projects_business}} |
| **Crawled pages / month** | {{pages_free}} | {{pages_solo}} | {{pages_pro}} | {{pages_business}} |
| **Max pages / crawl** | {{maxpages_free}} | {{maxpages_solo}} | {{maxpages_pro}} | {{maxpages_business}} |
| **AI fixes / month** | {{corrections_free}} | {{corrections_solo}} | {{corrections_pro}} | {{corrections_business}} |
| **Max files / fix** | {{files_free}} | {{files_solo}} | {{files_pro}} | {{files_business}} |
| **Core Web Vitals URLs / crawl** | {{pagespeed_free}} | {{pagespeed_solo}} | {{pagespeed_pro}} | {{pagespeed_business}} |
| **Written articles / month** | {{articles_free}} | {{articles_solo}} | {{articles_pro}} | {{articles_business}} |
| **Assistant messages / month** | {{assistant_free}} | {{assistant_solo}} | {{assistant_pro}} | {{assistant_business}} |
| **Team seats** | {{members_free}} | {{members_solo}} | {{members_pro}} | {{members_business}} |
| **Backlink opportunities** | — | ✓ | ✓ | ✓ |
| **Competitors** | — | — | ✓ | ✓ |

## How each quota is counted

### AI fixes — the rule that matters

**In GitHub: one unit = one file written by the model.**

A fix that resolves 300 URLs by modifying a single template costs **one** unit. The quota
measures the model's work, not the size of the problem.

On WordPress, the same rule applies page by page: each title or meta description that
{{app_name}} proposes counts as one unit as soon as it is prepared, before it is saved on
your site. You still review and confirm saving it. Text you write yourself and rewrites
that use no AI do not consume this quota.
→ [Fix in WordPress](/docs/corriger-dans-wordpress)

In practice, a medium-sized site uses a few dozen units in the first month —
the one where you catch up on the backlog — and far fewer afterward, once the template
issues are fixed.

The Free plan includes {{corrections_free}} AI fixes per month, on a compatible GitHub repository
or with WordPress and Yoast SEO,
as well as the audit and the PDF and CSV exports. The fix pack without a repository is available from the
Solo plan up. The limits actually available are shown in the table above.

### Written articles — a separate counter

**One unit = one page written**, regardless of its size and the number of
files touched: the section index modified along the way so that the page is not an orphan
is not a second article.

This counter is **separate from the fixes counter**, deliberately. An article has no common
measure with a title rewrite, and mixing them would make both quotas unreadable:
publishing two pages should not take away your fixes for the month.

The caps are deliberately low. Google's spam policy targets content produced
at scale for ranking, however it is produced — a generous cap
would turn this feature into a way to harm the very site it is meant to serve. See
[Write a page and propose it as a pull request](/docs/rediger-du-contenu).

### Team seats

A seat lets you invite one person to your projects. Seats are **flat-rate**: no extra
billing, no per-seat payment. What a collaborator uses on one of
your projects is charged to **your** account, with a record of who clicked. See
[Team accounts](/docs/comptes-d-equipe).

### Files per fix

The maximum number of files a single fix can touch. It protects against the forty-file
pull request that no one will review.

This cap is also reduced to your remaining quota: if you have 3 units left, a fix that
would touch 12 files stops at 3.

### Crawled pages

The monthly total of HTML pages crawled, across all projects. Resources — images, CSS,
scripts — are not included, unless you enable checking them, in which case they are
capped separately.

The actual cap of a crawl is the smallest of three numbers: the project's limit, the plan's
per-crawl limit, and what remains of the monthly quota.

### Core Web Vitals URLs

The number of URLs measured by PageSpeed on each crawl. The limit comes from Google's API,
which is slow and has a daily quota, and this limit is shared across the whole platform.

The product measures the first valid HTML pages in alphabetical order. You set their
number in the analysis settings, within your plan’s limit.

→ [Track performance](/docs/suivre-la-performance)

### Competitors

From the Pro plan up. Up to {{competitors_max}} competitors per project,
{{competitor_pages}} pages crawled per competitor, and an automatic refresh every
{{competitor_refresh_days}} days.

The threshold is higher than for the other modules because a competitor crawl spends
processing time on a site that is not yours.

→ [Analyze competitors](/docs/analyser-les-concurrents)

### Backlink opportunities

From the Solo plan up, with two separate monthly quotas: searches and message
drafts. They are deliberately low — useful link building is measured in a few relevant
contacts, not in hundreds of messages sent.

→ [Backlinks](/docs/backlinks)

## What is never limited

- Reading reports and history.
- All CSV and PDF exports. The fix pack is available from Solo, with no separate monthly quota.
- Content suggestions that you copy and paste yourself.
- Viewing issues and their URLs.

## Reaching a quota

The action in question is refused with a message that names the quota reached and points to the
**Subscription** page. Everything else keeps working: an exhausted fixes quota
does not prevent crawls, reports or exports.

Monthly counters use the UTC calendar month: they reset on the first day of each month at
00:00 UTC and do not carry over. This date is separate from your subscription renewal.
**Subscription** shows the tracked month and six monthly usages: pages, assistant messages,
fixes, articles, backlink searches and message drafts for those links. Sites are an active
capacity, not a monthly usage.

## Which account is affected?

The **Subscription** page shows the plan, usage counters and invoices of your personal account. If you came from a shared site, the owner's name and their plan are shown separately: buying a plan for yourself does not change the limits of their site. Contact that owner to change their subscription; the back button keeps you on the site in question.

Your preferences, your country, your password and your choice regarding the origin of your sign-up are in **My account**. Your Google, Bing, GitHub and Netlify connections remain available further down, through the shortcuts on that page. For the settings of a specific site, open its menu.

## Change plans

**Subscription** page. An upgrade takes effect immediately. A downgrade is scheduled
for the end of the current period and remains displayed until then — so you keep what you
paid for until the very end.
