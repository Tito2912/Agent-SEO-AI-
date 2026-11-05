---
title: "Compatible CMSs and platforms"
meta_title: "Compatible CMSs and platforms — {{app_name}} documentation"
description: "What {{app_name}} fixes on your platform: WordPress, Ghost, PrestaShop, Wix and Webflow connected; Shopify, Squarespace, Systeme.io guided; code on GitHub."
kind: "Documentation"
section: "Fixing"
order: 25
updated_at: "2026-11-04"
audience: "All plans"
keywords: ["CMS", "WordPress", "Ghost", "PrestaShop", "Shopify", "Wix", "Webflow"]
app_href: "/"
related: ["corriger-dans-wordpress", "corriger-dans-ghost", "corriger-dans-prestashop", "corriger-dans-wix", "fix-pack-sans-depot"]
faq:
  - question: "My CMS isn't on the list: what should I do?"
    answer: "Choose Other CMS. The audit, the reports and the explanation of each issue work the same way; only the steps specific to your tool are missing. Starting with the Solo plan, the fix pack provides the data to apply."
  - question: "Can I connect two CMSs to the same project?"
    answer: "No. A project has a single platform: choosing another one removes the existing connection."
  - question: "Is the CMS connection included in the free plan?"
    answer: "Yes. Suggested texts use your plan's AI fixes, {{corrections_free}} per month on the Free plan. A text you write yourself consumes nothing."
---

The audit works on every site: its address is enough. What changes from one platform to
another is the way you **fix**. {{app_name}} handles three cases.

## The three ways to fix

| Your site | What {{app_name}} does | What you need |
|---|---|---|
| Code on GitHub (Next.js, Astro, Nuxt, Hugo…) | prepares the fixes in a pull request, to review before publishing | a repository built with a supported technology |
| WordPress with Yoast SEO | writes and saves the SEO titles and meta descriptions of pages and posts | WordPress 6.9, Yoast SEO 28.7, an application password |
| Ghost | writes and saves the SEO titles and meta descriptions of posts and pages | Ghost 5, an Admin API key |
| PrestaShop | writes and saves the SEO titles and meta descriptions of products and categories | PrestaShop 8, a Webservice key |
| Wix | writes and saves the SEO titles and meta descriptions of pages and product pages | a published site, an API key with the SEO permission |
| Webflow | writes and saves the SEO titles and meta descriptions of static pages, live at your next publication | a published site, a site token (Pages write access) |
| Shopify, Squarespace, Systeme.io, WordPress with Rank Math | shows, for each issue, where to make the change in your tool | nothing to connect |

→ [Connect GitHub](/docs/connecter-github)

## Choose your platform

In the project, open **CMS and integrations**, answer **Which platform is your site
on?**, then choose **Save my platform**. This choice adapts the fixing instructions for
each issue. For WordPress, Ghost, PrestaShop, Wix and Webflow, the connection card appears on the same
screen.

A project has a single platform: choosing another one removes the existing connection.

## Connected CMSs

On WordPress, Ghost, PrestaShop, Wix and Webflow, the connection covers the **SEO title** and the
**meta description**. The flow is the same everywhere:

- {{app_name}} suggests a text, in the page's language and at the right length; you can
  also write your own;
- nothing is saved before you approve the preview;
- an issue that affects several pages is fixed in one batch, verified by a single analysis;
- each suggested text counts as one AI fix from your plan; a text you write yourself
  consumes nothing.

Each CMS has its own requirements and scope:

- [Fix in WordPress with Yoast SEO](/docs/corriger-dans-wordpress)
- [Fix in Ghost](/docs/corriger-dans-ghost)
- [Fix in PrestaShop](/docs/corriger-dans-prestashop)
- [Fix in Wix](/docs/corriger-dans-wix)
- [Fix in Webflow](/docs/corriger-dans-webflow)

## Guided platforms

On Shopify, Squarespace and Systeme.io, {{app_name}} doesn't write to your site.
Each issue shows the steps to follow in your tool, the screen to open and the field to fill
in, then the new analysis checks the result. The same goes for WordPress with Rank Math: the
connection goes through Yoast SEO, and for Wix blog posts, which Wix doesn't yet let another
application edit.

Starting with the Solo plan, the fix pack gathers the data to apply and how to apply it, for
you or for your provider.

→ [Fix pack: fix without a repository](/docs/fix-pack-sans-depot)

## What stays outside connected CMSs

Redirects, canonicals, structured data and page content don't go through the connection. The
guided instructions for each issue show where to make the change; on a compatible GitHub
repository, {{app_name}} prepares them in a pull request.

→ [Automatic fixes](/docs/corrections-automatiques)
