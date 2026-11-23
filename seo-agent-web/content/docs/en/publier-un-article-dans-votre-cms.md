---
title: "Publish an article to your CMS"
meta_title: "Publish an article to your CMS (WordPress, Ghost, Wix)"
description: "Send an article written in Noyaru to your CMS as a draft, review it, then publish it after a separate confirmation; what each CMS does and how to verify it."
kind: "Documentation"
section: "Going further"
order: 45
updated_at: "2026-11-23"
audience: "Pro and Business"
keywords: ["publish an article", "WordPress", "Ghost", "Webflow", "Wix Blog", "PrestaShop", "draft", "CMS"]
app_href: "/"
related: ["rediger-du-contenu", "cms-compatibles", "verifier-une-correction", "comptes-d-equipe"]
faq:
  - question: "Does {{app_name}} publish my article without asking me?"
    answer: "No. The first send saves a draft, once you have ticked that you reviewed the content and its destination. Publishing is a second step, with its own checkbox."
  - question: "Why is this connection separate from the one used for SEO fixes?"
    answer: "Because it needs other rights: creating and publishing an article, not only changing an SEO title or a meta description. Metadata-editing rights alone are not enough. It is connected and disconnected separately."
  - question: "What should I do if the result is \"to be confirmed\"?"
    answer: "Open your CMS and check whether the article exists. {{app_name}} never re-sends a creation on its own: it could create a duplicate. Then use \"Review the result in my CMS\", which reads without writing anything."
  - question: "Why doesn't my CMS show the layout of the preview?"
    answer: "Because your site's theme decides the final layout. The preview is for checking the text, the links and the business information, not the styling."
  - question: "Does publishing improve my search ranking?"
    answer: "It doesn't prove it. A new analysis must still confirm that the page is accessible and indexable; the result is then measured in the performance screen."
---

An article written in {{app_name}} can go straight to your CMS, with no copy and paste: you review
its preview, it is saved **as a draft**, then you publish it with a **separate confirmation**.
Nothing is written to your site before both of your approvals.

This connection is available as a pilot. It is for articles written in {{app_name}}; writing itself
is described in [Write an article and prepare its publication](/docs/rediger-du-contenu).

## What each CMS does

| CMS | What is created | What you need on the CMS side |
|---|---|---|
| WordPress | a **post** (draft status, then published) | the site's HTTPS address, a username and an **application password** whose account can edit and publish posts; Yoast is not required |
| Ghost | a **post** (draft, then published); no newsletter is sent | the site's HTTPS address and the **Admin API key** of a custom integration |
| Webflow | an **item in a collection** of your choice, as a draft then published | the site ID, an integration token, the destination collection and its **rich text** field |
| Wix | a **blog draft post**, then its publication | the site ID, an API key, and the ID of the **author member** in Wix |
| PrestaShop | a **CMS page** (not a blog post), inactive until published | the shop's HTTPS address, a web service key, the language and shop numbers |

Blogs provided by a PrestaShop module need an integration specific to that module: this flow
creates a CMS page, nothing else.

The key you enter is encrypted and never shown again. Use a key created for this purpose:
metadata-editing rights alone are not enough.

## Publish an article

1. In the project, open **Content**, then **Publish to my CMS**.
2. Choose your platform and **connect it**: {{app_name}} checks that the key opens this project's
   site before saving the connection.
3. Choose the **article** and its **language**, then what your CMS asks for (collection and text
   field for Webflow, author for Wix, language and shop for PrestaShop).
4. Choose **Prepare the publishing preview** and review the text, the links and the business
   information.
5. Tick that you reviewed the content and its destination, then choose **Save the draft in my
   CMS**.
6. Review the draft in your CMS if you wish, then, in {{app_name}}, tick **I confirm the publication
   of this version** and choose **Publish to my site**.

If you edit the draft in the CMS between the two steps, prepare a new review: {{app_name}} only
publishes the version you approved.

→ [Verify a fix](/docs/verifier-une-correction)

## Images

The images of a {{app_name}} article are private at first. For **WordPress** and **Ghost**, tick the
transfer authorization at step 3: they are sent to your CMS's media library, and the preview is
only prepared once each one is confirmed. If the CMS refuses one, a new attempt needs your express
approval, with a dedicated checkbox.

For **Webflow, Wix and PrestaShop**, transferring private images is not available yet: use images
already hosted on your site.

## What protects your site

- The content is checked **before anything is sent**: a text or an image the CMS could not accept
  is refused before the first write, leaving nothing behind.
- Draft and publication are two distinct confirmations, and each one keeps a receipt of your
  approval.
- If the CMS's reply is lost, the result stays **to be confirmed**: {{app_name}} never re-sends a
  creation on its own, to avoid a duplicate.
- **Review the result in my CMS** compares what the CMS shows with what you approved, without
  writing anything. If the draft changed on the CMS side, the check says so and you continue
  directly in your CMS.
- On Webflow, only the chosen item is published, not the rest of the site.

## After publishing

The CMS confirms the publication and {{app_name}} shows the page's address. A new analysis must
still confirm that the page is accessible and indexable: publishing alone proves no progress in
search ranking.

→ [Track performance](/docs/suivre-la-performance)

Members of a project's team can connect a CMS and publish.

→ [Team accounts](/docs/comptes-d-equipe)
