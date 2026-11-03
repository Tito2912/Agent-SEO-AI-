---
title: "Fix in PrestaShop"
meta_title: "Fix in PrestaShop: title tags and meta descriptions"
description: "Connect PrestaShop with a Webservice key, have the title tags and meta descriptions of products and categories written in each language, then verify the result."
kind: "Documentation"
section: "Fixing"
order: 28
updated_at: "2026-11-03"
audience: "All plans"
keywords: ["PrestaShop", "Webservice key", "title tag", "meta description", "product page"]
app_href: "/"
related: ["cms-compatibles", "verifier-une-correction", "plans-et-quotas", "comptes-d-equipe"]
faq:
  - question: "Why does PrestaShop refuse the connection?"
    answer: "Most often: a store running version 1.7, a disabled web service, a key without the products, categories and languages permissions, or an /api address that doesn't respond because URL rewriting is off at the host. The message displayed names the cause."
  - question: "My store has several languages: which translation is changed?"
    answer: "The one of the analyzed page, read from the start of its address (/fr/, /en/…). The other translations are sent back as they were. A multilingual store whose addresses don't state the language isn't changed."
  - question: "Why can't CMS pages be changed?"
    answer: "Because when it saves the title tag of a CMS page, PrestaShop also saves its content again and may alter it. For those pages, the issue's guided instructions show where to make the fix in PrestaShop."
  - question: "Can {{app_name}} change anything other than the title and description?"
    answer: "No. The connection only touches the title tag and meta description of published products and categories. For everything else, the guided instructions and the fix pack tell you what to change."
---

On a PrestaShop store, {{app_name}} fixes the **title tag** and **meta description** of
**products** and **categories** directly, without a GitHub repository, in each page's language:
it writes the text if you want it to, you review it, and nothing is written to the store before
you approve it. A new analysis then confirms the result.

This connection is available as a pilot.

## What you need

- **PrestaShop 8** or later: version 1.7 can't change a single field;
- the **web service** enabled, with a dedicated **Webservice key**;
- the HTTPS address of the store, on the same domain as the project;
- in a store with several languages, addresses that start with the language (`/fr/`,
  `/en/`…), as PrestaShop does by default.

The key is created in PrestaShop, under **Advanced Parameters → Webservice**: enable the web
service, then add a key named “{{app_name}}” with these permissions, and no others:

| Resource | Permissions to check |
|---|---|
| `products` | view (GET) and partial update (PATCH) |
| `categories` | view (GET) and partial update (PATCH) |
| `languages` | view (GET) |

If the store's `/api` address doesn't respond, URL rewriting must be enabled at the host; it
is on by default with Apache.

## Connect PrestaShop

1. In the project, open **CMS and integrations** and choose **PrestaShop** as the platform.
2. Enter the store address (with its subfolder if PrestaShop is installed in one) and the
   Webservice key.
3. Choose **Check and connect PrestaShop**.

{{app_name}} checks the PrestaShop version, the key's permissions and the store's languages
before saving the connection. The key is encrypted, never displayed again, and access is
checked again on every operation.

## Fix a page

From **Guided fix** or **Fixes**, choose **Prepare in PrestaShop**.

1. **Suggest a text**: {{app_name}} writes a value based on what the analysis read on the page
   (headings, subheadings), in the page's language and at the right length. You can also write
   the text yourself.
2. **See the before/after**: the preview shows the current value and the suggested value. It
   remains valid for 20 minutes.
3. **Apply on my site**: the product or category is read again just before writing; only the
   page's language changes, then it is read again to confirm the save.
4. **Verify the result**: a new analysis, counted against your usual page quota, confirms that
   the issue is gone.

→ [Verify a fix](/docs/verifier-une-correction)

### Pages that can be changed

{{app_name}} changes **published** product and category pages, at their usual address, which
contains their ID: `/en/12-linen-shirt.html` for a product, `/en/3-clothes` for a category. CMS
pages (delivery, legal notice…), the home page, brands and addresses with parameters (filters,
sorting, pagination) are fixed in PrestaShop, with the issue's guided instructions.

### What a suggestion costs

A suggested text counts as **one AI fix** from your plan, whatever the page's language. If it
can't find a text that is faithful to the page and fits the right length, {{app_name}} doesn't
suggest one and nothing is counted. A text you write yourself consumes nothing.

→ [Plans and quotas](/docs/plans-et-quotas)

## Fix every page of an issue

When an issue affects several pages, its detail page offers **Fix these pages in PrestaShop**.
The texts are written one page after another; you review them on a single screen, uncheck the
pages to leave aside, then save the selection in one go. **A single check analysis** then
verifies all the saved pages. A batch shows up to 100 pages at a time.

## What protects your store

- Nothing is written without an approved preview, and only the title tag and meta description
  of published products and categories can be written.
- **The other languages are kept**: each write sends back every translation of the field,
  unchanged except the page's own. If PrestaShop doesn't provide them all, nothing is written.
- The product or category is read again just before writing: if it has changed since the
  preview, the operation stops to preserve your changes.
- If PrestaShop's response gets lost or reports an error, the operation remains **To
  confirm**: {{app_name}} reads the values again, without ever rewriting automatically.
- **Restore the previous value** undoes a change if the metadata hasn't changed since it was
  applied.

If the check analysis still reads the old value even though PrestaShop did save the new one, a
cache is serving the old version of the page: the tracking says so instead of reporting a
failure. Clear PrestaShop's cache (**Advanced Parameters → Performance**) and your CDN's, then
run the check again.

## What the connection doesn't do

CMS pages, the home page, redirects, friendly URLs and structured data remain outside its
scope: the guided instructions for each issue show where to make the change.

→ [Compatible CMSs and platforms](/docs/cms-compatibles)

A project's team members can connect PrestaShop and apply fixes.

→ [Team accounts](/docs/comptes-d-equipe)
