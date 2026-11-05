---
title: "Fix in Webflow"
meta_title: "Fix in Webflow: SEO titles and meta descriptions"
description: "Connect Webflow with a site token, have the SEO titles and meta descriptions of static pages written, publish in Webflow, then verify the result."
kind: "Documentation"
section: "Fixing"
order: 30
updated_at: "2026-11-04"
audience: "All plans"
keywords: ["Webflow", "Webflow site token", "SEO title", "meta description", "Webflow publish"]
app_href: "/"
related: ["cms-compatibles", "verifier-une-correction", "plans-et-quotas", "comptes-d-equipe"]
faq:
  - question: "Why doesn't {{app_name}} publish my site?"
    answer: "Because a Webflow publication puts live everything waiting in the Designer, including work you haven't finished. {{app_name}} therefore saves its fixes and lets you publish when you decide; the recommended token can't publish anyway."
  - question: "Why does Webflow refuse the connection?"
    answer: "Most often: a token without write access to Pages, a deleted token, a token from another site, or a site published at an address other than the project's. The message displayed names the cause."
  - question: "Why can't blog posts be changed?"
    answer: "Posts, like every page of a CMS collection, take their title and description from the collection template, shared by all its items. Changing it would change every post. For these pages, the issue's guided instructions show where to make the fix in Webflow."
  - question: "Can {{app_name}} change anything other than the title and description?"
    answer: "No. The connection only touches the SEO title and meta description of static pages; only the field being fixed is sent to Webflow."
---

On a Webflow site, {{app_name}} fixes the **SEO title** and the **meta description** of
**static pages** directly, without a GitHub repository: it writes the text if you wish, you
review it, and nothing is written before you approve it. {{app_name}} **never publishes your
site**: the fix waits for your next publication in Webflow, then a new analysis confirms the
result.

This connection is available as a pilot.

## What you need

- a **published** Webflow site, at the address the project analyzes (custom domain or
  `webflow.io` address);
- a **site token** created in the site settings, with the permissions below.

The token is created in Webflow, in the site settings (**Site settings**), under
**Apps & integrations → API access**: generate a token named “{{app_name}}” with these
permissions, and no other:

| Permission | Level to choose |
|---|---|
| Pages | read and write (*Read and write*) |
| Sites | read-only (*Read-only*): it can't publish |

Copy the token: Webflow shows it only once.

## Connect Webflow

1. In the project, open **CMS and integrations** and choose **Webflow** as the platform.
2. Paste the site token.
3. Choose **Check and connect Webflow**.

{{app_name}} checks the token's permissions and that the site is published at the project's
address before saving the connection. The token is encrypted, never shown again, and access is
checked again for every operation.

## Fix a page

From **Guided fix** or **Fixes**, choose **Prepare in Webflow**.

1. **Suggest a text**: {{app_name}} writes a value based on what the analysis read on the page
   (headings, subheadings), in the page's language and at the right length. You can also write
   the text yourself.
2. **See the before/after**: the preview shows the current value and the suggested value. It
   remains valid for 20 minutes.
3. **Apply on my site**: the page is read again just before writing; only the field being fixed
   is sent to Webflow, then the page is read again to confirm the save.
4. **Publish in Webflow**: in the Designer, choose **Publish**. The fix goes live at that
   moment, together with your other pending changes.
5. **Verify the result**: a new analysis, counted against your usual page quota, confirms that
   the issue is gone.

→ [Verify a fix](/docs/verifier-une-correction)

### Pages that can be changed

{{app_name}} changes published **static pages**, home page included, in the site's main
language. Items of a **CMS collection** (blog posts, listings…) take their title and description
from the collection template, shared by all: they are fixed in Webflow, with the issue's guided
instructions. Drafts, utility pages (404, protected page) and other languages also remain
outside the connection.

### What a suggestion costs

A suggested text counts as **one AI fix** from your plan. If it can't find a text that is
faithful to the page and fits the right length, {{app_name}} doesn't suggest one and nothing is
counted. A text you write yourself consumes nothing.

→ [Plans and quotas](/docs/plans-et-quotas)

## Fix every page of an issue

When an issue affects several pages, its detail page offers **Fix these pages in Webflow**. The
texts are written one page after another; you review them on a single screen, uncheck the pages
to leave aside, then save the selection in one go. **A single publication** in Webflow puts
them all live, and **a single check analysis** then verifies them. A batch shows up to 100
pages at a time.

## What protects your site

- Nothing is written without an approved preview, and only the SEO title and meta description
  of static pages can be written.
- **Nothing is published**: your other changes in progress in the Designer stay pending as long
  as you don't publish.
- The page is read again just before writing: if it has changed since the preview, the
  operation stops to preserve your changes.
- If Webflow's response gets lost or reports an error, the operation remains **To confirm**:
  {{app_name}} reads the values again, without ever rewriting automatically.
- **Restore the previous value** undoes a change if the metadata hasn't changed since it was
  applied; like a fix, the rollback waits for your publication.

If the check analysis still reads the old value even though Webflow did save the new one, the
change is probably not published yet: the tracking says so instead of reporting a failure.
Publish your site in Webflow, then run the check again.

## What the connection doesn't do

Collection pages, other languages, redirects, page addresses and structured data remain outside
its scope: the guided instructions for each issue show where to make the change.

→ [Compatible CMSs and platforms](/docs/cms-compatibles)

A project's team members can connect Webflow and apply fixes.

→ [Team accounts](/docs/comptes-d-equipe)
