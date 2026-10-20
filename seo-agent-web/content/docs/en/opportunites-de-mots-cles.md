---
title: "Keyword opportunities"
meta_title: "Keyword opportunities — {{app_name}} documentation"
description: "The three types of opportunities detected in your Search Console data, how they are calculated, and rewriting a page as a pull request."
kind: "Documentation"
section: "Search Console and keywords"
order: 31
updated_at: "2026-10-19"
audience: "All plans, Search Console required"
keywords: ["keywords", "Search Console", "CTR", "average position", "rewrite"]
app_href: "/"
featured: true
related: ["connecter-search-console", "suivre-la-performance", "corrections-automatiques"]
faq:
  - question: "Where do these keywords come from? An SEO tool's database?"
    answer: "No. They are your own Search Console queries, the ones your site already appears for. No estimated search volume, no third-party database: only facts from your account."
  - question: "Why are queries in positions 1 to 3 missing?"
    answer: "Because there is nothing to promise on them. A query already in the top three has a click-through rate close to its ceiling: presenting it as an opportunity would be selling hot air."
  - question: "Is the estimated click gain reliable?"
    answer: "It is a projection based on the click-through rate your own site achieves in its top positions, not on a market average. It is only shown if that point of comparison exists, and it remains an estimate."
  - question: "Does the rewrite change my page's content?"
    answer: "No. It only touches the title and the meta description. The page appearing in search results does not guarantee the quality of its content; the proposal only covers how the page is presented."
---

This is the feature that answers the question “what do I work on now?” with something other
than a hunch.

The starting point isn't a purchased keyword database: it's **your** Search
Console queries, the ones for which Google already shows your site. The product isn't looking for
keywords to conquer, it's looking for the ones you're narrowly losing.

Prerequisite: [Search Console connected](/docs/connecter-search-console) to the project.

## The three types of opportunities

{{app_name}} asks Search Console for the query **and** the page together. That's what makes the
difference between a remark and an action: an opportunity that names only the query leaves you
hunting for the page, while one that names both can be fixed.

### Seen, never clicked

Position 3 to 10 — Google's first page — and **zero clicks** over the period.

You're visible and nobody comes in. It isn't a ranking problem, it's a
`title` and description problem: what searchers read doesn't match what they
were looking for.

This is the most profitable case of the lot: the work is already done, only the shop window is missing.

### Clicked less than usual

First page, some clicks, but a click-through rate well below what **your site**
usually gets in those positions.

The comparison is internal, and that matters: comparing your click-through rate to a market
average means nothing, because yours depends on your industry, your brand and the
type of results displayed. Comparing your page to your other pages does mean something.

The threshold is set at half of your benchmark.

### Close to page 1

Position 10 to 20. You're on the second page, so invisible in practice — but the gap can
be closed.

Here, unlike the other two, the answer is rarely a tag: it's content, internal
links to this page, or an external link.

## What the product refuses to do

**It ignores queries under 50 impressions.** Below that, a click-through rate has no statistical
meaning: two impressions and one click is 50%, and that means nothing.

**It ignores positions 1 to 3.** A query already at the top has a click-through rate close to its
ceiling. Promising a gain on it would be dishonest.

**It only shows an estimated gain if it has a benchmark.** The projected gain is calculated from
the click-through rate your site actually achieves in its top positions. If the site has
no top positions, no figure is shown — “no estimate available” and
“no gain expected” are not the same answer.

**It sorts by impressions, not by estimated gain.** Impressions are a fact from your account;
the gain is a model. Sorting on the fact rather than on the model avoids putting the most
optimistic projections at the top of the list.

## Track a query

**Track** button on a row. The query and its target page join your watch list,
which lets you find them again from one crawl to the next without redoing the analysis.

## Rewrite the page as a pull request

This is the action that closes the loop. Search Console names the query and the page; {{app_name}}
rewrites that page's `title` and `meta description` to answer that query, and
opens a pull request on your repository.

The page **keeps its topic**. The proposal covers the title and the description; appearing in search results is not enough to conclude that the content is good or that it converts.

This feature is deliberately more cautious than the issue fixer, because no
crawl can validate its result:

- **The target file comes only from the repository's route map.** The model never chooses
  the file: if there is no certain match, the rewrite is refused.
- **The values are written by the model, and the pull request says so.** It is never
  merged automatically, even in direct apply mode.
- **A page whose `title` is assembled dynamically is refused** rather than guessed.
- **Only one open pull request per page**, not per query: two queries targeting the same
  page rewrite the same two lines, and the second PR would conflict with the
  first.

Prerequisite: a [connected repository](/docs/connecter-github). The “Prepare a rewrite” button first opens a summary of the page, the search and the cost. You then confirm creating the draft, and review the changes on GitHub. **Each written file** counts against the AI fix quota; creating the draft does not merge the proposal.

## How to know if it worked

Not with a crawl. The crawler will see that the text has changed, which proves nothing.

These tasks are therefore excluded from
[automatic verification](/docs/verifier-une-correction). The verdict is read in Search
Console, three to six weeks later, on two figures: the clicks and the average position
of that query.

Keep in mind Google's publishing delay — two to three days — and the time it
needs to recrawl the page.
