---
title: "Write an article and prepare it for publishing"
meta_title: "SEO content writing — {{app_name}} documentation"
description: "Write an article to review in HTML and Markdown or prepare a GitHub proposal; understand per-language quotas and the publishing steps."
kind: "Documentation"
section: "Going further"
order: 44
updated_at: "2026-10-19"
audience: "Pro and Business"
keywords: ["AI writing", "SEO content", "new page", "pull request", "automatic mode"]
app_href: "/"
related: ["analyser-les-concurrents", "connecter-github", "verifier-une-correction", "plans-et-quotas"]
faq:
  - question: "Why is there no preview before the pull request?"
    answer: "Because the pull request IS the preview. It opens as a draft: you read the full diff, your repository's build gives its verdict, and nothing is published until you merge. A separate preview would either charge you for a discarded draft, or let pages be generated for free in a loop."
  - question: "Can the agent write about a topic my site already covers?"
    answer: "Automatic mode picks topics that were not found in your analyzed pages. Check the scope of the analysis: a page may exist elsewhere on your site. When you enter the topic yourself, you choose it."
  - question: "How many pages does the agent write on its own?"
    answer: "It depends on the frequency you choose: one page per month, every two weeks, every week or twice a week. Each language version counts toward the monthly quota of the account that manages the project."
  - question: "What happens if the agent doesn't know where to put the page?"
    answer: "It refuses and tells you so. Creating a folder structure on your site based on a guess is worse than admitting it doesn't know. It needs at least one existing page in the same section to transpose its convention from."
---

{{app_name}} prepares a text for you to review, based on a topic and an existing section. Choose **Content only** to get Markdown and HTML that you publish yourself, or a **draft GitHub proposal** if the site's code is connected.

## Where to find the screen

**Content**, in a project's menu, from the Pro plan up. **Content only** mode requires a site analysis so it knows the site's sections and pages. It works with your CMS, without a GitHub repository. **GitHub proposal** mode and automatic writing require a [connected repository](/docs/connecter-github).

## Write a page by hand

Choose the destination, the **topic**, its **section**, then the length, tone and languages if needed. The summary shows the number of versions and what they cost.

In **Content only** mode, review the isolated preview or the Markdown text, then copy or download the version you want. Copying does not publish the article: paste it into your CMS afterward and check the live page.

In **GitHub proposal** mode, also fill in an **address**. The address is suggested from the topic, and you
adjust it — the section (`/blog`, `/guides`, `/ressources`) is an editorial decision that
no one can make for you.

![Writing form: destination, topic, length, tone, site section, address and languages](/static/captures/rediger-une-page.webp "The page writing form.")

What happens next, in order:

1. The agent reads your repository's tree and **transposes the path of a sibling page** — an
   existing page in the same section. It does not try to guess your
   generator's convention: it copies the one you already use.
2. It reads that sibling page and has the new one written **in the same form**: same header keys,
   same language, same tags. A page missing a key that its siblings have is
   rejected, not published.
3. It **adds the entry** to the section's index, by cloning a sibling's entry.
4. It opens the pull request **as a draft**.

### What the agent checks before writing

| check | what it rejects |
|---|---|
| Placement | a section with no existing page, or an address your site already serves |
| Header keys | a page that lacks the keys its siblings have — it would break your build |
| File review | an unbalanced file: braces, tags, front matter |
| Addresses | a `canonical` or an `og:url` that does not point to the page itself |
| Escaping | a backslash placed in markup text, where it would be displayed as is |

The last two exist because they actually happened: a model readily copies the
`canonical` of the page it is shown, and a green build cannot see an address that lies.

## The link from the section

A page that nothing links to is an orphan, and your next crawl would flag it. The agent
distinguishes three cases, and tells you which one applies in the body of the pull request:

- **the index lists its pages by hand** → the entry is added, cloned from a sibling's;
- **the list is generated** (a template that loops over the folder) → there is nothing to add;
- **it doesn't know** (ambiguous, unreadable or missing index) → the pull request opens anyway,
  stating plainly that the page is not linked from anywhere.

The proposed pages log, at the bottom of the screen, keeps this information: a row marked
**"to link by hand"** flags a page whose entry still needs to be added.

## Automatic mode

Off by default. To turn it on, you must **name the section** where the pages will go: no one will
review the address before the pull request, and `/blog` versus `/guides` cannot be guessed.

![Automatic mode settings: section, frequency, day, time, time zone, length, tone, translations and publishing](/static/captures/mode-automatique.webp "The automatic mode settings.")

Writing follows your frequency, day, time and time zone, within the available quotas. It only prepares a page:

- on a topic that one of your competitors covers and that was not found in the analyzed pages of your site — see
  [Analyze competitors](/docs/analyser-les-concurrents);
- if a crawl of your own site exists, since otherwise every topic would look uncovered;
- on a topic **never proposed before**, even if you had closed its pull request;
- and **only if it knows how to link the page**. That is the only difference from manual mode: here
  no one will read the warning, so a page it cannot link is not written at
  all.

Publishing is set separately: **With your approval**, the proposal waits for your review and then your permission to merge; with **Automatic · merge after checks**, it can be merged once the repository's checks pass. If there are no usable checks, or if they fail, your approval is requested.

An approved proposal can therefore remain pending. A merged proposal is not proof that the page is live: that depends on your site's deployment.

An account whose plan has changed can always **turn off** the mode, whatever its plan.

## What it costs

Each **language version counts as one unit**, regardless of the page's size and the number of files
touched — the index modified along the way is not a second article. The counter is **separate from
the fixes counter**: publishing two pages does not take away your fixes for the month.

| plan | articles per month |
|---|---|
| {{label_free}} | {{articles_free}} |
| {{label_solo}} | {{articles_solo}} |
| {{label_pro}} | {{articles_pro}} |
| {{label_business}} | {{articles_business}} |

These caps are deliberately low. Google's spam policy targets **content produced
at scale** whose primary purpose is ranking, however it is produced.
A generous cap would turn this feature into a way to harm the very site it is meant to serve.

## What this page does not do

- **Manual** writing publishes nothing: you copy the content into your CMS, or review the draft GitHub proposal before it is merged. **Automatic** writing follows the explicit publishing setting described above.
- It does **not check facts**. Figures, names, dates, prices and sales promises must
  be checked before merging. The text is plausible; it is not verified.
- It does not write the **first** page of a section. Without a sibling page, your
  site's convention for that section does not exist yet, and the agent holds back rather than inventing one.

Once the page is merged, [checking that a fix actually worked](/docs/verifier-une-correction)
applies to it just as it does to everything else: the next crawl tells you whether it is really live,
indexable and linked.
