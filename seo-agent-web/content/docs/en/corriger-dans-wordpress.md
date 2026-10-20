---
title: "Fix in WordPress with Yoast SEO"
meta_title: "Fix in WordPress and Yoast SEO — {{app_name}} documentation"
description: "Connect WordPress and Yoast SEO, have titles and meta descriptions written, fix every page of an issue in one batch, then verify the result."
kind: "Documentation"
section: "Fixing"
order: 25
updated_at: "2026-10-20"
audience: "All plans"
keywords: ["WordPress", "Yoast SEO", "SEO title", "meta description", "application password"]
app_href: "/"
related: ["verifier-une-correction", "fix-pack-sans-depot", "plans-et-quotas", "comptes-d-equipe"]
faq:
  - question: "Why does WordPress refuse the connection?"
    answer: "Most often: a WordPress version older than 6.9 or a Yoast SEO version older than 28.7, an account without permission to edit Yoast metadata, or an incorrect application password. The message displayed names the cause."
  - question: "Is a suggested text counted if I don't apply it?"
    answer: "Yes: the suggestion is a text written by the model, so it counts as one AI fix as soon as it's produced. If no text fits within the right length, nothing is suggested and nothing is counted."
  - question: "Can I undo a change?"
    answer: "Yes, from the change's tracking: Restore the previous value puts the old value back, provided the page's metadata hasn't changed since."
  - question: "Can {{app_name}} change anything other than the title and description?"
    answer: "No. The connection only touches the Yoast SEO title and meta description of published pages and posts. For everything else, the guided instructions and the fix pack tell you what to change."
---

On a WordPress site equipped with Yoast SEO, {{app_name}} fixes **SEO titles** and
**meta descriptions** directly, without a GitHub repository: it writes the text if you
want it to, you review it, and nothing is written to your site before you approve it. A
new analysis then confirms the result.

This connection is available as a pilot.

## What you need

- **WordPress 6.9** or later, and **Yoast SEO 28.7** or later;
- a WordPress account allowed to edit Yoast metadata;
- an **application password** for that account;
- the HTTPS address of WordPress, on the same domain as the project and reachable from the Internet.

The application password is created in WordPress, under **Users → Profile → Application
Passwords**. Create a dedicated access, named for example “{{app_name}}”: you'll be able to
revoke it on its own, without touching your usual password.

## Connect WordPress

1. In the project, open **CMS and integrations** and choose **WordPress** with **Yoast SEO**.
2. Enter the WordPress address (with its subfolder if it's installed in one), the username
   and the application password.
3. Choose **Check and connect WordPress**.

{{app_name}} checks access and the Yoast version before saving the connection. The
credentials are encrypted, never displayed again, and access is checked again on every
operation. **Disconnect WordPress** removes the connection from the project; also revoke the
application password in WordPress if you no longer plan to use it.

## Fix a page

From **Guided fix** or **Fixes**, choose **Prepare in WordPress**.

1. **Suggest a text**: {{app_name}} writes a value based on what the analysis read on
   the page (headings, subheadings), in the page's language and at the right length. You
   can also write the text yourself.
2. **See the before/after**: the preview shows the current value and the suggested value. It remains
   valid for 20 minutes.
3. **Apply on my site**: the page is read again in WordPress just before writing, then
   read again afterwards to confirm the save.
4. **Verify the result**: a new analysis, counted against your usual page quota,
   confirms that the issue is gone.

→ [Verify a fix](/docs/verifier-une-correction)

### What a suggestion costs

A suggested text counts as **one AI fix** from your plan, just like a file written
to a repository. If it can't find a text that is faithful to the page and fits the right length,
{{app_name}} doesn't suggest one and nothing is counted. A text you write yourself
consumes nothing.

→ [Plans and quotas](/docs/plans-et-quotas)

**Duplicate** titles and descriptions aren't written for you: the right text must stand out
from those of the other pages, and it's up to you to write it.

## Fix every page of an issue

When an issue affects several pages, its detail page offers **Fix these pages in
WordPress**. The batch screen groups three steps:

1. **Have the texts written**: the suggestions are written one page after another. The
   batch stops when your plan has no AI fixes left; a page without a suggestion
   doesn't stop the others.
2. **Review and save**: edit a text if needed, uncheck the pages to leave
   aside, then save the selection in one go. Each page is read again and written
   separately: a page modified in WordPress in the meantime is stopped on its own.
3. **Verify the result**: **a single check analysis** verifies all the saved
   pages, and the result is displayed page by page. Resolved pages remain visible
   on the batch screen for 30 days.

A batch shows up to 100 pages at a time.

## What protects your site

- Nothing is written without an approved preview, and only the SEO title and meta description of
  published pages and posts can be written.
- The page is read again just before writing: if it has changed since the preview, the operation
  stops to preserve your changes.
- If WordPress's response gets lost, the operation remains **To confirm**: {{app_name}} reads
  the values again, without ever rewriting automatically.
- **Restore the previous value** undoes a change if the metadata hasn't
  changed since it was applied.

## “Your site still shows the old version”

Most WordPress sites have a page cache. If the check analysis still reads
the old value even though WordPress did save the new one, the tracking says so instead
of reporting a failure: clear the cache (plugin, host or CDN), then run the
check again. There's no need to rewrite the text.

## What the connection doesn't do

Redirects, canonicals, structured data and sites running Rank Math remain
outside its scope. The guided instructions for each issue show where to make the
change, and the fix pack provides the data to apply yourself.

→ [Fix pack: fix without a repository](/docs/fix-pack-sans-depot)

A project's team members can connect WordPress and apply fixes,
just as they can propose fixes on GitHub.

→ [Team accounts](/docs/comptes-d-equipe)
