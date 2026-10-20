---
title: "Fix without a Git repository: the fix pack"
meta_title: "Fix pack: fixing without a repository — {{app_name}} documentation"
description: "The archive of fixes to apply on WordPress, Shopify, Webflow or any site without an accessible repository: its contents, its format, how to use it."
kind: "Documentation"
section: "Fixing"
order: 24
updated_at: "2026-10-20"
audience: "All plans"
keywords: ["fix pack", "WordPress", "Shopify", "Webflow", "without a repository"]
app_href: "/"
related: ["corrections-automatiques", "anomalies-et-priorites", "exports-et-rapports"]
faq:
  - question: "Does the fix pack use up my AI fix quota?"
    answer: "No. It's an export built from the crawl data. The fix quota only counts files written by the model in a repository."
  - question: "Can I import the redirects CSV directly into my host?"
    answer: "With hosts that accept a rules file — Netlify, Vercel, Cloudflare — the conversion is immediate. On WordPress, a redirect plugin usually accepts a CSV import. Always check a few lines before applying in bulk."
  - question: "Does the provided robots.txt replace mine?"
    answer: "It's a template, not a replacement. Compare it with yours before overwriting anything: your current robots.txt may contain deliberate rules that the template doesn't know about."
---

Not every site has an accessible Git repository. WordPress managed by a third party, Shopify,
Webflow, Wix, or simply code you don't have rights to: the
[automatic pull request fix](/docs/corrections-automatiques) is then out of
reach.

The fix pack exists for these cases. It's a ZIP archive built from the latest crawl,
containing what needs to be applied and the instructions for doing it.

## Download it

Project overview → **Fix Pack ZIP**. The archive covers the latest available crawl;
run a new crawl first if yours is out of date.

No fix quota is consumed: it's an export, not model work.

## What's inside

```
README.md                         Context: site, crawl date, what the pack contains
HOW_TO_APPLY.md                   Instructions, by platform
meta.json                         The pack's metadata
robots.txt                        A suggested robots.txt, with the Sitemap line
exports/pages_seo.csv             One row per page, all the tags observed
exports/issues_summary.csv        Issues: type, severity, volume, priority, effort
exports/redirects_observed.csv    Redirects actually encountered, with their chain
exports/redirects_to_fill.csv     404s to redirect — the destination column is yours
exports/sitemap_urls.txt          The list of indexable URLs, to turn into a sitemap
schema/website.jsonld             WebSite structured data template
schema/organization.jsonld        Organization structured data template
```

Two files deserve a closer look.

### `pages_seo.csv`

One row per page, with what the crawler saw: `title`, `meta_description`, `canonical`,
`lang`, `meta_robots`, `x_robots_tag`, first `h1` and number of `h1`, word count, number
of images and how many lack `alt`, response size, response time.

This is the file you really work with. Open it in a spreadsheet, sort on the
`title` column to see the duplicates show up in one block, filter the empty
`meta_description` values, spot the `canonical` tags that don't point to the row's URL.

### `redirects_to_fill.csv`

The list of 404 URLs, with an **empty** destination column. This is deliberate: choosing
where to redirect a deleted page is an editorial decision, not a calculation.

Fill in the column, then import the file into your host or your redirect
plugin.

!!! warning "Don't redirect everything to the home page"
    It's the most common reflex and the most costly one: a mass redirect to the
    home page is treated as a disguised error page. Redirect to the closest page,
    or leave the 404 when there's no equivalent.

## Apply, by platform

The `HOW_TO_APPLY.md` file details each case. In short:

**WordPress** — `title`, description and canonicals via your SEO plugin (Yoast, Rank Math)
or the editor. Redirects via a dedicated plugin, which usually accepts a CSV import.
With Yoast SEO, titles and descriptions can also be fixed directly from
{{app_name}}: see [Fix in WordPress](/docs/corriger-dans-wordpress).

**Shopify** — `title` and descriptions in the admin, product by product or by import. The
structured data lives in the theme, most often in `theme.liquid`.

**Webflow** — tags in the page and collection settings. Redirects in the
project settings.

**Coded sites without a connected repository** — apply the changes in code; then, if you end up
connecting the repository, automatic fixing will take over and verify the following crawls.

## Verify afterwards

The fix pack leaves the product: {{app_name}} doesn't know what you did with it. The
[automatic verification](/docs/verifier-une-correction) therefore doesn't apply.

The method stays the same, done manually: run a new crawl after applying the batch, and
compare it with the previous one. The issues you handled should be gone, and the
counter of newly appeared issues should stay at zero.

→ [Read the report](/docs/lire-le-rapport)
