---
slug: "corriger-le-seo-dans-le-code-pas-dans-un-rapport"
title: "Fix SEO in the code, not in a report"
meta_title: "Fix SEO issues directly in your source code"
description: "Why the gap between an audit and a fixed site is the real problem in technical SEO, and what a fix delivered as a pull request looks like."
kind: "Guide"
updated_at: "2026-09-30"
published_at: "2026-09-03"
audience: "Technical teams, freelancers and agencies"
keywords: ["automated SEO fixes", "pull request", "technical SEO", "actionable SEO audit", "client-side SEO fix", "SEO patch"]
featured: true
related: ["audit-seo-technique-checklist-priorites", "regression-seo-apres-mise-en-production", "corriger-title-meta-description-grande-echelle"]
cta: "That's exactly what {{app_name}} does: it locates the template responsible, writes the fix in your repository, opens a pull request, and checks on the next crawl that the issue is gone."
faq:
  - question: "Can an AI really change my code safely?"
    answer: "You manage the risk with constraints, not with trust. Restricted file extensions, a cap on the number of files, fixes refused when the target file is uncertain, and above all a pull request that a human reviews before merging."
  - question: "How is this different from a content suggestion?"
    answer: "A suggestion gives you the text to write; you still have to find the file, edit it, test it and deploy it. The fix takes care of those four steps and leaves you the fifth: saying yes."
  - question: "What if my site doesn't have a Git repository?"
    answer: "The approach doesn't apply as is. You still have the export of ready-to-apply fixes, with instructions for each platform — slower, but with the same prioritization work done upfront."
  - question: "Why not use a script that fixes the page on the fly?"
    answer: "Because then the fix belongs to the service: the subscription ends, the script stops loading, and the page goes back to what it was. Your source code is still wrong, so the next developer reproduces the issue. It's useful when the repository is out of reach; it's not the same thing as a fix you own."
---

The problem with technical SEO has never been finding the issues. Any crawler will list
hundreds of them in an hour.

The problem is the distance between that list and a fixed site. That distance is measured
in weeks, and it's where most audits go to die.

## The funnel where everything gets lost

The usual path of a detected issue:

1. The crawler flags it.
2. Someone reads it and decides it matters.
3. Someone figures out **which file** produces that HTML.
4. Someone writes the change.
5. Someone tests it.
6. Someone deploys it.
7. Someone checks that it worked.

Steps 3 through 6 require access to the code and developer time — the scarcest resource in
any organization. Step 7 almost never happens.

The result: a 230-line report produces about ten fixes in a quarter. It's not a motivation
problem, it's a unit-cost problem.

## What changes when the fix arrives as a pull request

The path becomes:

![Issues that are candidates for a fix, each with a preview and a button to create a pull request](/static/captures/accueil-corrections.webp "Every fixable issue leads to a pull request.")

1. The crawler flags it.
2. Someone decides it matters.
3. **A pull request exists.**
4. Someone reviews three lines of diff and merges.
5. The next crawl confirms it.

The expensive steps disappear, and above all the seventh — verification — becomes
automatic, because it's tied to the next crawl.

Step 4 stays human, and it should. A three-line diff takes thirty seconds to review, and
that's what makes the rest acceptable.

## The hard part: finding the right file

This is where it all comes down to, and it's the part demos gloss over.

A production HTML page doesn't tell you which file produced it. In between, there's a site
generator, templates, components, layers of configuration.

The deciding question is always the same: **does the issue come from the page or from the template?**

- 120 product pages without a `meta description` → the template doesn't output it. One change,
  one file, 120 report rows gone.
- 3 articles without a description → three pieces of content to fill in, page by page.

Getting the side wrong produces either a pointless fix or 120 changes where one would have
done the job.

And when the mapping isn't certain, the right answer is to **refuse the
fix**. A patch applied to the wrong file costs more to undo than it would have cost never to
make it.

## What a machine can write, and what it shouldn't

The line is clear, and it runs between two families.

**Deterministic fixes.** An inconsistent `canonical`, a missing reciprocal `hreflang`, an
internal link that goes through a redirect, a redirect loop. The value to write isn't a matter of
opinion: it's derived from the crawl. The model's job is limited to inserting that value
correctly into your code.

**Editorial fixes.** A `title`, a `meta description`, an `alt`. Here, the model is genuinely
writing something. It's useful, often good — and it deserves a review, because plausible
text isn't necessarily accurate text.

The practical consequence: serious automation treats these two families differently.
Merging a deterministic fix without review is defensible; merging a sentence written by a
model without review is not.

## Fixing in the code, or fixing on the fly

There's another way to automate: don't touch the repository at all. A script injected
into your pages, or a proxy placed in front of your site, rewrites the HTML as it goes out
to the visitor. The missing `title` appears, the `canonical` gets corrected, and the report
turns green.

It's worth saying what this approach really solves, because it's real: it requires no
access to the code, it installs in minutes, and it works when the site has no Git
repository or when the dev team isn't available. For a site you can't modify, it's the
only option.

But it falls short in three ways.

**The fix belongs to the service, not to you.** The subscription ends, the script stops
loading, the page goes back to exactly what it was. You haven't built anything up: you were
renting the result.

**Your code is still wrong.** The template that didn't output a `meta description` still
doesn't. The next developer who opens it will reproduce the issue on the pages that follow,
because nothing in the repository tells them there's a problem. The rendering layer
hides the defect instead of removing it.

**One more moving part between the visitor and the page.** Whatever does the rewriting can break, slow things down,
or get it wrong — and anything that runs in the browser assumes the bot reading your site
actually executes the script.

The test that settles it comes down to one question: **what's left if you cancel your subscription
tomorrow?** A merged pull request lives in your repository, in your history, reviewed and
approved by someone on your team. It doesn't evaporate along with the vendor that proposed it,
and it stays true even if you switch tools — or stop using any at all.

It's the same criterion as for the rest of this article: a fix counts when it outlives
whatever produced it.

## And verification, which is missing everywhere

A merged pull request is not a fixed issue. In between, there's a deployment, a cache,
sometimes a CDN, and the assumption that the patch was the right one.

The only proof that counts is a crawl run **after** the deployment that no longer sees
the issue.

And you also need to look in the other direction: which issues **appeared** between the
two crawls? A `canonical` fix that resolves 40 URLs and pushes 12 into `noindex` is a
failure, even if the total went down.

That's the full loop: detect, decide, fix, deploy, verify, repeat. As long as one step is
missing, the audit remains a document.

## What it doesn't replace

Not your content strategy, not the choice of which pages deserve your time, not your
understanding of your market. A machine that fixes `canonical` tags doesn't know which
pages make you money.

It just removes the work that should never have taken weeks.
