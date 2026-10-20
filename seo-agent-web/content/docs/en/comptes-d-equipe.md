---
title: "Team accounts: invite someone to your projects"
meta_title: "Team accounts — {{app_name}} documentation"
description: "Invite a collaborator, what they see and can do, who pays for their actions, and the two connections that are not shared the same way."
kind: "Documentation"
section: "Account and billing"
order: 51
updated_at: "2026-10-20"
audience: "Pro and Business"
keywords: ["team accounts", "collaborator", "agency", "invitation", "seats"]
app_href: "/settings/team"
related: ["plans-et-quotas", "connecter-github", "connecter-search-console", "depannage"]
faq:
  - question: "Who pays for what a collaborator uses?"
    answer: "The project owner. A consultant on the Free plan who works on a Business project gets the Business engine, charged to the agency's account. It is the PROJECT that determines the plan and the quota, not the person who is logged in."
  - question: "Does my collaborator keep their own subscription?"
    answer: "Yes. Outside the project — their assistant, their billing, their own sites — they stay on their own account and plan. Attaching everything to the host would have erased the subscription of a consultant who is a customer in their own right."
  - question: "How many seats do I have?"
    answer: "{{members_pro}} on the {{label_pro}} plan and {{members_business}} on {{label_business}}. Seats are flat-rate: inviting someone does not trigger any additional payment."
  - question: "Can someone belong to two teams?"
    answer: "No, one membership per person. Two memberships would make an address like /projects/my-site ambiguous if both accounts owned a project with that name."
---

A team account lets you invite someone — a consultant, a colleague, a client — to
work on **your** projects, without giving them your credentials.

## Invite someone

From **Your account → Team**. You enter an email address, the person receives an
invitation, and they join your account whether or not they already have a {{app_name}} account.

| plan | included seats |
|---|---|
| {{label_free}} | {{members_free}} |
| {{label_solo}} | {{members_solo}} |
| {{label_pro}} | {{members_pro}} |
| {{label_business}} | {{members_business}} |

Seats are **flat-rate**: no additional billing, no per-seat payment.
A pending invitation can be revoked, a member can be removed, and a member can
leave your account on their own.

## What a member sees

They open **your projects** just as you do: crawl reports, issues, fixes, keywords,
competitors, backlinks. They also see **the jobs in progress** on these projects — a crawl started
by another member appears in their list, and they can follow it, cancel it or run it again.

They also still see **their own projects**, if they have any. The two lists coexist.

## Who pays, and with which engine

**The project decides.** On a project that belongs to you, a collaborator gets
*your* plan — your fix engine, your file limits, your quotas — and every
action is charged to **your** account.

Outside the project, they stay on their own: their assistant, their billing and their own sites follow their
own subscription. This is deliberate: a consultant may also be a {{app_name}} customer in their own right,
and joining an agency must not erase their subscription.

Every charge records **who clicked**, in addition to the account charged. See
[Plans and quotas](/docs/plans-et-quotas).

## External connections: two rules, not one

This is the most important point on this page, and it is not symmetrical.

**GitHub is an agency credential.** A member who starts a fix pushes to the repository
with the **account owner's GitHub token**. The pull requests therefore carry the agency's account,
not the collaborator's. This is a knowingly accepted consequence:
the GitHub card on the screen says so explicitly, so that no one discovers it in
the history of a client repository. See [Connect GitHub to a project](/docs/connecter-github).

**A Search Console or Bing property belongs to the site owner.** These connections are
therefore **per project**: each project carries its own, and a member can connect the client's Search
Console account on that client's project. Otherwise, the account connection serves as a fallback, which
spares an agency that has only one account from reconnecting everything. See
[Connect Google Search Console](/docs/connecter-search-console).

## One membership per person

A person can only join one account at a time. The reason is concrete: if you and
the agency each owned a project named `my-site`, the address `/projects/my-site` would become
ambiguous. In that specific case, **your** project opens — the only choice that surprises
no one.

## What this page does not do

- There are **no roles**. A member has the same rights as you on the account's projects,
  fixes and pull requests included. Only invite people you would trust with
  your repository.
- Seats cannot be purchased individually. To get more, you need to change plans.
- Removing a member removes their access to the account's projects, but **does not delete** the
  jobs they started or the pull requests they opened.


## If the link opens with the wrong account

The page shows the invited address in masked form and the account that is currently logged in. Click **Log in with the invited account**: you will be logged out, then logging in will bring you back to the same link. Use the address that received the invitation. An expired or already used link does not grant any access; ask the owner for a new link if needed.

In **Team**, the account you joined and the members of your own account are shown separately. Leaving a team removes your access to its sites; the sites remain with their owner.
