---
title: "Fix in Ghost"
meta_title: "Fix in Ghost: SEO titles and meta descriptions"
description: "Connect Ghost with an Admin API key, have the SEO titles and meta descriptions of posts and pages written, fix them in a batch, then verify the result."
kind: "Documentation"
section: "Fixing"
order: 27
updated_at: "2026-11-03"
audience: "All plans"
keywords: ["Ghost", "Admin API key", "custom integration", "SEO title", "meta description"]
app_href: "/"
related: ["cms-compatibles", "verifier-une-correction", "plans-et-quotas", "comptes-d-equipe"]
faq:
  - question: "Why does Ghost refuse the connection?"
    answer: "Most often: a Ghost version older than 5, an incomplete Admin API key (it looks like id:secret), the key of another Ghost site, or an address that is neither the site's nor its .ghost.io admin address. The message displayed names the cause."
  - question: "Can I fix the home page?"
    answer: "Not from {{app_name}}: Ghost sets the home page's title and description in its general settings, under Meta data, not post by post."
  - question: "Can {{app_name}} change anything other than the title and description?"
    answer: "No. The connection only touches the SEO title and meta description of published posts and pages. For everything else, the guided instructions and the fix pack tell you what to change."
  - question: "How do I remove {{app_name}}'s access?"
    answer: "Delete the custom integration in Ghost: its key stops working immediately. Disconnecting Ghost in {{app_name}} also removes the connection from the project."
---

On a Ghost site, {{app_name}} fixes the **SEO titles** and **meta descriptions** of posts and
pages directly, without a GitHub repository: it writes the text if you want it to, you review
it, and nothing is written to your site before you approve it. A new analysis then confirms the
result.

This connection is available as a pilot.

## What you need

- **Ghost 5** or later, hosted by Ghost(Pro) or by you;
- an **Admin API key**, from a custom integration;
- the HTTPS address of the site, on the same domain as the project, or its `.ghost.io` admin
  address.

The key is created in Ghost, under **Settings → Integrations → Add custom integration**. Name
the integration “{{app_name}}” and copy the **Admin API key** displayed. Deleting this
integration removes {{app_name}}'s access, without touching your other integrations.

## Connect Ghost

1. In the project, open **CMS and integrations** and choose **Ghost** as the platform.
2. Enter the Ghost address and the Admin API key.
3. Choose **Check and connect Ghost**.

{{app_name}} checks the Ghost version, that the address really is the project's site and that
the key is accepted, before saving the connection. The key is encrypted, never displayed again,
and access is checked again on every operation.

## Fix a page

From **Guided fix** or **Fixes**, choose **Prepare in Ghost**.

1. **Suggest a text**: {{app_name}} writes a value based on what the analysis read on the page
   (headings, subheadings), in the page's language and at the right length. You can also write
   the text yourself.
2. **See the before/after**: the preview shows the current value and the suggested value. It
   remains valid for 20 minutes.
3. **Apply on my site**: the post is read again in Ghost just before writing, then read again
   afterwards to confirm the save.
4. **Verify the result**: a new analysis, counted against your usual page quota, confirms that
   the issue is gone.

→ [Verify a fix](/docs/verifier-une-correction)

### What a suggestion costs

A suggested text counts as **one AI fix** from your plan. If it can't find a text that is
faithful to the page and fits the right length, {{app_name}} doesn't suggest one and nothing is
counted. A text you write yourself consumes nothing.

→ [Plans and quotas](/docs/plans-et-quotas)

## Fix every page of an issue

When an issue affects several pages, its detail page offers **Fix these pages in Ghost**. The
texts are written one page after another; you review them on a single screen, uncheck the pages
to leave aside, then save the selection in one go. **A single check analysis** then verifies all
the saved pages. A batch shows up to 100 pages at a time.

## What protects your site

- Nothing is written without an approved preview, and only the SEO title and meta description
  of published posts and pages can be written.
- The post is read again just before writing: if it has changed since the preview, the
  operation stops to preserve your changes.
- If Ghost's response gets lost, the operation remains **To confirm**: {{app_name}} reads the
  values again, without ever rewriting automatically.
- **Restore the previous value** undoes a change if the metadata hasn't changed since it was
  applied.

If the check analysis still reads the old value even though Ghost did save the new one, a cache
(CDN or host) is serving the old version of the page: the tracking says so instead of reporting
a failure. Clear that cache, then run the check again.

## What the connection doesn't do

The home page is set in Ghost's general settings, under **Meta data**. Tag and author pages,
redirects and structured data remain outside its scope: the guided instructions for each issue
show where to make the change.

→ [Compatible CMSs and platforms](/docs/cms-compatibles)

A project's team members can connect Ghost and apply fixes.

→ [Team accounts](/docs/comptes-d-equipe)
