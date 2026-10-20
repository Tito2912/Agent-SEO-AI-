---
title: "Connect GitHub to a project"
meta_title: "Connect a GitHub repository — {{app_name}} documentation"
description: "Authorize {{app_name}} on GitHub, link a repository and a branch to a project, and choose between Review mode and Full Access mode."
kind: "Documentation"
section: "Fixing"
order: 21
updated_at: "2026-10-20"
audience: "Solo, Pro and Business"
keywords: ["GitHub", "pull request", "repository", "OAuth"]
app_href: "/settings/accounts#github-connect-card"
related: ["corrections-automatiques", "verifier-une-correction", "fix-pack-sans-depot", "plans-et-quotas"]
faq:
  - question: "What permissions does {{app_name}} request on GitHub?"
    answer: "Read access to your profile and email, and access to repositories. Write access is required to create a branch and open a pull request; without it, the product can only suggest text."
  - question: "Can I limit access to a single repository?"
    answer: "The GitHub OAuth token covers the repositories your account has access to. If you want to isolate access, the usual practice is to create a GitHub service account, invite it only to the relevant repository, and connect that account."
  - question: "What happens if I disconnect GitHub?"
    answer: "The token is deleted and the fix buttons disappear. Pull requests already open stay on your repository: they belong to you, and {{app_name}} no longer touches them."
  - question: "Can “Full Access” mode break my site?"
    answer: "It automatically merges deterministic fixes — never those written by the model, never a redirect change. The risk is therefore limited, but it remains: on a branch that deploys automatically, unreviewed code goes live. Stay in Review mode if you are not sure."
---

This is the connection that turns {{app_name}} from a tool that flags into a tool that fixes.
It is optional — and it is the feature the product exists for.

## Authorize GitHub on your account

**My account → Connections → GitHub → Connect**. You go to GitHub, you authorize, you
come back. The card then shows the connected account and the list of accessible repositories.

The permissions requested: read access to your profile and email, and access to repositories. The
latter is essential — creating a branch and opening a pull request is a write operation.

!!! note "A service account, if you want to isolate access"
    The token carries the permissions of **your** GitHub account. In an organization with many
    repositories, the clean way to restrict access is to create a dedicated GitHub account,
    invite it to the site's repository only, and connect that one.

A “manual token” mode also exists, for cases where the platform OAuth is not
available. It does the same thing and is configured on the same page.

## Link a repository to a project

The GitHub connection is at the **account** level. The link is at the **project** level:
one project, one repository, one branch.

Open **Analysis settings → GitHub (optional)** for the site concerned. Three settings:

- **Repository** — in `organization/repository` format. Pick it from the list of accessible
  repositories.
- **Branch** — `main` by default. This is the branch {{app_name}} starts from, and the
  target of its pull requests.
- **Mode** — `Review` or `Full Access`.

## The two modes, and which one to choose

### Review (default)

{{app_name}} creates a branch, writes the fix to it, opens a pull request and gives you
the link. **Nothing is merged without you.**

You get everything a PR brings: the line-by-line diff, your continuous integration checks,
the deployment preview if your host generates one, and the history.

This is the mode to keep, even when you trust the result. Reviewing a
three-line diff takes thirty seconds.

### Full Access

The pull request is created the same way, then **merged automatically** (squashed) —
but only when all three of the following conditions are met:

- **no file was written by the model.** As soon as a value is written by the AI rather than
  derived from the crawl, the PR stays open and waits for your review;
- **no redirect rule or hosting configuration file was touched.** A
  routing change always goes through a human;
- **the fix does not rely on any assumption that needs to be validated.**

In other words, automatic merging is reserved for deterministic fixes, whose written
value comes from the crawl and not from the model. Everything else is up to you, whatever the
mode.

Even so, reserve Full Access for a site whose deployment pipeline you control — or for
a branch that does not go straight to production.

## What {{app_name}} won't do

The safeguards are not cosmetic, because the product writes to your code:

- **Branch names are validated** before any API call — no `..`, no
  absolute path, no `.lock` suffix, none of the forms that Git rejects or interprets.
- **Editable extensions are limited** to template and configuration files:
  HTML, JSX/TSX, Vue, Svelte, Astro, PHP, Markdown, JSON, YAML, TOML, and hosting
  configuration files. No binaries, no files outside this list.
- **The number of files touched per fix is capped**, and this cap depends on your
  plan. A fix that would rewrite forty files stops at the limit rather than
  producing a PR that nobody will review.
- **Calls are rate-limited**: single fixes and full-issue fixes
  are capped at 20 per hour, the global fix at 5 per hour.

## Netlify

A Netlify connection is also available, on the same page. It is used to list the sites in
your Netlify account so you can link them to projects — not to deploy. Deployment remains
driven by your usual pipeline, triggered by merging the pull request.

## Check that everything is in place

On the project's **Fixes** page, the top banner shows the repository, the branch and the
mode. If it says “No GitHub repository connected”, the fix buttons will not appear
on the issue detail sheets.

→ [Automatic fixes](/docs/corrections-automatiques)


## Remove the repository from a single site

In the same panel, **Disconnect** removes the link after you confirm. Your personal GitHub connection and the repositories linked to your other sites remain available. Proposals already open on GitHub are not deleted.

The confirmation shown follows the server's response. If a response is lost, **Refresh status** checks the saved link before you try again; this check creates no fix and no proposal.
