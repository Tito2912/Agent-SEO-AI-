---
title: "Verify that a fix actually worked"
meta_title: "Verify a fix — {{app_name}} documentation"
description: "How {{app_name}} confirms at the next crawl that a fixed issue is gone, detects collateral regressions, and what each status means."
kind: "Documentation"
section: "Fixing"
order: 23
updated_at: "2026-10-20"
audience: "Solo, Pro and Business"
keywords: ["verification", "SEO regression", "fix tracking", "check crawl"]
app_href: "/"
related: ["corrections-automatiques", "corriger-dans-wordpress", "lire-le-rapport", "automatiser-les-audits"]
faq:
  - question: "Why does a fix stay “in progress” after the PR is merged?"
    answer: "Because verification needs a crawl that happens after the deployment. Until the live site has been crawled again, the product has no proof and refuses to make one up."
  - question: "My fix is marked “still present” — what does that mean?"
    answer: "The fix is in the repository but the issue persists on the crawled site. The three usual causes: the PR isn't merged, the deployment didn't happen, or a cache is still serving the old version."
  - question: "Can one fix break another?"
    answer: "Yes, and that's exactly what verification looks for. It compares the report with the reference report and flags the issues that appeared between the two, without attributing them to a specific fix when several share the same window."
---

A merged pull request is not a fixed issue. Between the two there's a
deployment, a cache, sometimes a CDN, and the assumption that the fix was the right one.

So {{app_name}} doesn't tick anything off at merge time. It waits for the next crawl and looks.

## With WordPress and Yoast SEO

Tracking keeps four steps: **Prepare → Review → Apply → Verify**.

1. Choose **Suggest a text** to have {{app_name}} write it, or type the SEO title
   or meta description yourself, then choose **See the before/after**.
2. Review the preview and choose **Apply on my site** to save the value.
3. On the same screen, choose **Verify the result**. If your site uses a cache,
   refresh it first. This check runs a new analysis and uses your usual
   page quota; an analysis already in progress is followed without starting a second one.
4. The result appears automatically on this screen: **Fix verified**, **The issue
   is still present**, or a reason explaining why the check couldn't reach a conclusion.
   If the analysis still read the old value even though WordPress saved the new one,
   the screen shows **Your site still shows the old version**: that's a cache to clear,
   not a text to rewrite.

For several pages of the same issue, **Fix these pages in WordPress** groups the
review and runs a single check analysis for all of them.
→ [Fix in WordPress with Yoast SEO](/docs/corriger-dans-wordpress)

You can leave the tracking and come back to it from **Guided fix → Track my
fix**, or in **CMS and integrations → Your latest WordPress operations**.
The **Restore the previous value** section lets you undo the change if the
metadata hasn't changed since it was applied.

After a confirmed result, **Choose another fix** opens the issues from the latest
report. The previous result is kept while you choose. The **Restore the previous value**
button remains available from the guided fix's success screen.
In **Fixes**, the remaining issues show their URL; the same issue can
be resolved on one page and persist on another. Verified fixes are
kept in the history, with a link to their WordPress operation.

## How it works

At the end of each crawl, the product takes the project's fix tasks and checks
each one against the report just produced:

- the fix's URLs were successfully revisited in a more recent crawl and
  the issue is gone → **resolved**;
- the issue persists, including on another page for a global fix or as
  an indexability variant → **still present**;
- URLs weren't crawled, the crawl is partial, or the fix's scope is
  unknown → **not verifiable**. A URL missing from the examples isn't enough to conclude.

A task confirmed as resolved automatically moves to **Done**. You have nothing to tick.

!!! note "The “PR open, issue still there” case"
    It's expected, not flagged. As long as the pull request isn't merged, the live
    site obviously hasn't changed — flagging a regression here would be noise.

## Statuses, and what they mean

| Status | Meaning |
| --- | --- |
| **Resolved** | The later crawl no longer sees the issue on this URL. That's the only proof that counts. |
| **Still present** | The fix is in the repository, the issue is still live. See below. |
| **Not verified** | No crawl has run since the fix. Start one. |
| **Not verifiable** | The crawl doesn't cover the fix well enough to conclude. Check the reason on the task and run a full crawl again. |

The project's **Fixes** page shows these counters at the top: how many verified
fixes are resolved, how many are still present, how many carry a pull request.

## “Still present”: the three causes

In order of frequency:

1. **The PR hasn't been merged.** Check the link on the task.
2. **The deployment didn't happen**, or it failed. The repository is up to date, the live
   site isn't.
3. **A cache is still serving the old version.** CDN, page cache, service worker. Open
   the URL in a private window and look at the source code: if the fix is there, it's a
   cache; if it isn't, it's a deployment.

If none of the three explains the result, the fix didn't target the right place. The
issue's detail page shows exactly what the crawler sees — that's the starting point for
understanding.

## Collateral regressions

Verification doesn't stop at the question asked. It compares the report with the **reference
report** — the one from the crawl on which the fix was decided — and flags the
issues that **appeared** between the two.

This is what catches the nasty case: a `canonical` fix that resolves 40 URLs and
puts 12 others in `noindex`.

Two precautions in how to read it, and they're deliberate:

- **When several fixes share the same window**, the product doesn't name a
  culprit. It shows the number of fixes involved and leaves you the diff. Blaming
  a PR at random would be worse than saying nothing.
- **When the reference report is unreadable or missing**, no comparison is
  shown. Publishing a diff against an empty baseline would make every surviving issue
  look new — a spectacular false positive.

## The special case of keyword rewrites

A page rewritten to better answer a query can't be validated by a crawl. The
crawler sees that the text changed; it can't tell whether the ranking moved.

These tasks are therefore **excluded** from automatic verification. Their verdict is read in
Search Console, a few weeks later, from clicks and average position.

→ [Keyword opportunities](/docs/opportunites-de-mots-cles)

## Close the loop without thinking about it

Verification requires a crawl after the fix. The simplest approach is to no longer
have to think about it: a scheduled crawl does the job, and you find your tasks moved to
“Done” on their own.

→ [Automate your audits](/docs/automatiser-les-audits)
