---
title: "Add a project"
meta_title: "Add a site to audit — {{app_name}} documentation"
description: "Create a project in {{app_name}}: start URL, subdomains, multilingual sites, staging environments and project deletion."
kind: "Documentation"
section: "Getting started"
order: 11
updated_at: "2026-09-03"
audience: "All plans"
keywords: ["project", "add a site", "domain", "subdomain"]
app_href: "/"
related: ["demarrer", "lancer-un-crawl", "plans-et-quotas"]
faq:
  - question: "Can I audit a password-protected staging site?"
    answer: "Not through interactive HTTP authentication. A staging site reachable through an unprotected secret URL works, as long as it responds with a 200 and does not block the crawler in robots.txt."
  - question: "Does a subdomain count as a separate project?"
    answer: "Yes, by default: the crawl stays on the host of the start URL. You can enable “Allow subdomains” in the crawl settings to cover blog.example.com from the example.com project."
  - question: "What happens if I delete a project?"
    answer: "The project, its crawls, its issues and its fix tasks are deleted. Pull requests already open on your GitHub repository are not: they belong to you."
---

A project represents **one site**. It holds the start URL, the crawl settings, the audit
history, the issues, the fix tasks and the connections (Search Console, Git
repository) that are specific to it.

## Create the project

From the dashboard, **Add a site**. Only one field is required: the start URL.

A few rules that prevent unpleasant surprises:

- **Give the canonical URL, with the right protocol and the right prefix.** `https://example.com`
  and `https://www.example.com` are not the same host for the crawler. If your site
  redirects one to the other, start from the destination, not the origin — otherwise the first
  hop of every crawl is a redirect.
- **Start from the home page**, unless you have a specific reason. The crawler follows links: a deep
  start URL will never see the sections that are only linked from the home page.
- **One project per domain.** Page quotas, scores and history only make sense when they are
  kept separate.

The number of simultaneous projects depends on your plan — see [Plans and quotas](/docs/plans-et-quotas).

## Multilingual sites

A site at `example.com/fr/` and `example.com/en/` remains **a single project**: both versions
share the same host, and `hreflang` checks only make sense if the crawler sees both
sides of the relationship. Splitting the languages into two projects breaks the detection of
missing reciprocal `hreflang` tags.

On the other hand, `example.fr` and `example.com` are two hosts, and therefore two projects.

## Subdomains

By default, the crawl does not leave the start host: a link to `blog.example.com` from
`example.com` is treated as an external link (checked, but not explored).

To cover subdomains in the same audit, enable **Allow subdomains**
in the project's [crawl settings](/docs/lancer-un-crawl). Watch the volume: a blog
on a subdomain can double the number of pages, and therefore the consumption of your monthly
quota.

## Staging and test environments

By default, {{app_name}} rejects private hosts (`localhost`, `127.0.0.1`, local network
addresses). This is a protection against requests to internal infrastructure, not a
cosmetic limitation.

A public staging site — a Netlify preview URL, a `staging.` subdomain —
can be audited normally. Just check two things:

1. It does not return a global `X-Robots-Tag: noindex`, otherwise the report will rightly
   tell you that all pages are non-indexable.
2. Its `robots.txt` does not disallow everything. If it does and you still want to
   audit it, the **Ignore robots.txt** option is available in the crawl settings — to
   be used only on a site you are responsible for.

## Delete a project

The project name is chosen at creation. Deletion, on the other hand, is permanent: it takes
with it the project's crawl history, reports, issues and fix tasks.

What it does **not** delete: pull requests already open on your repository,
fixes already merged, and exports you have downloaded.

## After creation

The project appears in the sidebar with its own pages: overview, crawl settings,
performance, keywords, competitors, backlinks, issues, fixes, automation
and history. They all stay empty until the first crawl has run.

→ [Run a crawl](/docs/lancer-un-crawl)
