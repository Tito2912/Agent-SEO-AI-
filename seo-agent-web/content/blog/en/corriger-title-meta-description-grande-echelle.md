---
slug: "corriger-title-meta-description-grande-echelle"
title: "Fixing title tags and meta descriptions at scale"
meta_title: "Fix title tags and meta descriptions in bulk"
description: "Handle 400 missing or duplicate tags without losing three weeks: tell template issues from page issues, and fix them at the source."
kind: "Tutorial"
updated_at: "2026-09-03"
published_at: "2026-05-13"
audience: "Service websites, e-commerce stores and blogs"
keywords: ["title tag", "meta description", "duplicate title tags", "on-page SEO"]
related: ["audit-seo-technique-checklist-priorites", "connecter-google-search-console-audit-mensuel"]
cta: "{{app_name}} identifies the template at fault, writes the fix in your code and opens a pull request — one change for 300 URLs, reviewed by you before it's merged."
faq:
  - question: "How long should a title tag be?"
    answer: "Google displays about 580 pixels, which works out to 55 to 60 characters depending on the letters used. Counting characters is a handy approximation; what matters is that the decisive information comes first, not last."
  - question: "Does the meta description affect rankings?"
    answer: "No, not directly. It affects click-through rate, which decides whether your ranking turns into a visit. For a page in position 4, that's the difference between a 3% and an 8% click-through rate."
  - question: "Google rewrites my title tags anyway, so why bother polishing them?"
    answer: "Google mostly rewrites them when they're poor: too short, too repetitive, or unrelated to the query. A precise, relevant title is kept in the vast majority of cases."
---

An audit flags 400 duplicate `title` tags and 250 missing `meta description` tags. Handled page
by page, that's three weeks of work. Handled properly, it's a single day.

The difference comes down to one question, asked before you write a single tag.

## The question: template or page?

**If every page of the same type is affected**, the cause is in the template. One product page
without a description happens; 1,200 product pages without a description means the template
isn't outputting the tag.

**If a few scattered pages are affected**, it's a content problem, and you need to handle them
one by one.

Do this triage before anything else. It decides whether your project takes an hour or three
weeks, and most audits that “take three weeks” simply never asked the question.

A quick test: sort your export by URL type. If the issues cluster around a pattern —
`/product/…`, `/blog/…`, `/category/…` — you've found your template.

## Fixing a template: the generic pattern

On a template, you don't write a tag: you write a **formula**, with a fallback for when a piece
of data is missing.

```
Title      : {page name} - {brand}
Product    : {product} - {category} | {brand}
Category   : {category}: {key benefit} | {brand}
Article    : {article title} - {blog}
```

Three rules that make all the difference:

1. **The decisive information first.** “Brand | Category | Product name” wastes the first
   thirty characters, the ones that are always displayed.
2. **An explicit fallback.** If the variable is empty, the template must still produce
   something sensible, not “- |”.
3. **A variable that guarantees uniqueness.** Without it, the formula produces 200 identical
   titles — you've moved the problem, not solved it.

## What to fix first

Not all missing tags are created equal.

**First, pages that get impressions.** Search Console gives you the list. A missing description
on a page with 12,000 impressions costs you clicks every day; on an archive page with zero
impressions, it costs nothing.

**Next, business pages.** Services, categories, comparisons, local pages. They may get few
impressions precisely because they're poorly titled.

**Then, visible duplicates.** Two pages with the same `title` compete with each other in the
index, and Google picks one — rarely the one you would have picked.

**Everything else can wait.** A missing description on a tag page with no potential isn't worth
opening a ticket for.

## Writing the remaining tags

For the pages your templates don't cover, four criteria:

- **Match the intent, not the keyword.** A page that answers “how much does X cost” should
  include a price or a price range in its `title`.
- **Stay within the displayed length.** About 55–60 characters for the `title`, 150–160 for the
  description. Anything longer gets truncated.
- **Don't repeat the `h1` word for word.** They're two different surfaces: the `title` is read in
  a list of ten competing results, the `h1` is read by someone who has already arrived.
- **Give people a reason to click** in the description: what the page delivers, not what it
  contains.

## Checking that it worked

Two checks, on two time scales.

**Right away**: run a new crawl. The targeted issues should be gone, and the count of **new**
issues should stay at zero. A badly written template formula can create 400 identical titles
where there used to be 400 missing ones.

**Three to six weeks later**: Search Console, on the affected pages. Click-through rate is the
only judge. Mind the lag — Google reports data two to three days late, and it has to recrawl
the page before even that.

## The full-automation trap

Generating 1,200 descriptions with an AI model and publishing them without review gives you
1,200 correct, interchangeable sentences. That's no better than 1,200 missing descriptions —
Google will rewrite both.

The right split: **formulas for volume, hand-written tags for the pages that matter.** Your
twenty business pages deserve twenty minutes each; your 1,200 product pages deserve a good
formula.
