---
title: "Fix in Wix"
meta_title: "Fix in Wix: SEO titles and meta descriptions"
description: "Connect Wix with an API key limited to SEO, have the SEO titles and meta descriptions of pages and product pages written, then verify the result."
kind: "Documentation"
section: "Fixing"
order: 29
updated_at: "2026-11-04"
audience: "All plans"
keywords: ["Wix", "Wix API key", "SEO title", "meta description", "product page"]
app_href: "/"
related: ["cms-compatibles", "verifier-une-correction", "plans-et-quotas", "comptes-d-equipe"]
faq:
  - question: "Why does Wix refuse the connection?"
    answer: "Most often: a key without the SEO permission, a key limited to another site, the ID of another site, or a site published at an address other than the project's (a custom domain and the wixsite.com address are not interchangeable). The message displayed names the cause."
  - question: "Do I need to republish the site after a fix?"
    answer: "No. The page is updated both in the Wix editor and on the live site, without publishing your other pending changes. The site must, however, have been published at least once."
  - question: "Why can't blog posts be changed?"
    answer: "Because Wix currently refuses any change to a blog post made by another application, even to its SEO settings alone. For these pages, the issue's guided instructions show where to make the fix in Wix."
  - question: "Can {{app_name}} change anything other than the title and description?"
    answer: "No. The connection only touches the SEO title and meta description of published pages and product pages. The page's other SEO settings are sent back as they were."
---

On a Wix site, {{app_name}} fixes the **SEO title** and the **meta description** of **pages**
and **product pages** directly, without a GitHub repository: it writes the text if you wish,
you review it, and nothing is written to the site before you approve it. A new analysis then
confirms the result.

This connection is available as a pilot.

## What you need

- a **published** Wix site, at the address the project analyzes (custom domain or free
  `wixsite.com` address);
- an **API key** created in your Wix account, limited to this site, with the **SEO** permission
  only;
- the **site ID**.

The key is created in your Wix account, on the **API Keys** page
(`manage.wix.com/account/api-keys`), with these settings:

| Setting | What to choose |
|---|---|
| Name | “{{app_name}}” |
| Sites | **Specific sites**, then only the project's site |
| Permissions | uncheck **All permissions**, then check only **SEO** |

Copy the key: Wix shows it only once. The site ID appears in the address of its dashboard:
`manage.wix.com/dashboard/<site-id>/…`.

## Connect Wix

1. In the project, open **CMS and integrations** and choose **Wix** as the platform.
2. Enter the site ID and the API key.
3. Choose **Check and connect Wix**.

{{app_name}} checks that the key opens this site's SEO settings, published at the project's
address, before saving the connection. The key is encrypted, never shown again, and access is
checked again for every operation.

## Fix a page

From **Guided fix** or **Fixes**, choose **Prepare in Wix**.

1. **Suggest a text**: {{app_name}} writes a value based on what the analysis read on the page
   (headings, subheadings), in the page's language and at the right length. You can also write
   the text yourself.
2. **See the before/after**: the preview shows the current value and the suggested value. It
   remains valid for 20 minutes.
3. **Apply on my site**: the page's SEO settings are read again just before writing; the page
   is updated in the Wix editor and on the live site, without publishing the rest of the site,
   then read again to confirm the save.
4. **Verify the result**: a new analysis, counted against your usual page quota, confirms that
   the issue is gone.

→ [Verify a fix](/docs/verifier-une-correction)

### Pages that can be changed

{{app_name}} changes the site's **pages**, home page included, and the Wix store's **product
pages**, in the site's main language. **Blog posts** are fixed in Wix, with the issue's guided
instructions: Wix doesn't yet let another application change them. The other languages of a
multilingual site also remain outside the connection.

### What a suggestion costs

A suggested text counts as **one AI fix** from your plan. If it can't find a text that is
faithful to the page and fits the right length, {{app_name}} doesn't suggest one and nothing is
counted. A text you write yourself consumes nothing.

→ [Plans and quotas](/docs/plans-et-quotas)

## Fix every page of an issue

When an issue affects several pages, its detail page offers **Fix these pages in Wix**. The
texts are written one page after another; you review them on a single screen, uncheck the pages
to leave aside, then save the selection in one go. **A single check analysis** then verifies
all the saved pages. A batch shows up to 100 pages at a time.

## What protects your site

- Nothing is written without an approved preview, and only the SEO title and meta description
  of published pages and product pages can be written.
- **The page's other SEO settings are kept**: Wix replaces all of a page's own SEO settings on
  every write; {{app_name}} reads them just before and sends back unchanged everything that
  isn't the field being fixed (social sharing, canonical URL, structured data…).
- The settings are read again just before writing: if they have changed since the preview, the
  operation stops to preserve your changes.
- If Wix's response gets lost or reports an error, the operation remains **To confirm**:
  {{app_name}} reads the values again, without ever rewriting automatically.
- **Restore the previous value** undoes a change if the metadata hasn't changed since it was
  applied; a page that had no value of its own gets back the one from Wix's SEO template.

A change is usually live in less than a minute. If the check analysis still reads the old value
even though Wix did save the new one, the tracking says so instead of reporting a failure: run
the check again a few minutes later.

## What the connection doesn't do

Blog posts, the other languages of a multilingual site, redirects, page addresses and
structured data remain outside its scope: the guided instructions for each issue show where to
make the change.

→ [Compatible CMSs and platforms](/docs/cms-compatibles)

A project's team members can connect Wix and apply fixes.

→ [Team accounts](/docs/comptes-d-equipe)
