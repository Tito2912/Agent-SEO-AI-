---
title: "Exports and reports"
meta_title: "CSV and PDF exports — {{app_name}} documentation"
description: "Every export available in {{app_name}}: summary report, issues, URLs per issue, fix pack, and which one to choose depending on the recipient."
kind: "Documentation"
section: "Going further"
order: 43
updated_at: "2026-09-03"
audience: "All plans"
keywords: ["CSV export", "PDF report", "client audit", "SEO reporting"]
app_href: "/"
related: ["lire-le-rapport", "fix-pack-sans-depot", "anomalies-et-priorites"]
faq:
  - question: "Do exports use up a quota?"
    answer: "No. They are built from data the crawl has already collected. Only fixes written by the model use up a quota."
  - question: "Can I export several crawls to compare them?"
    answer: "Each export covers one crawl. To compare, export both and line them up in a spreadsheet — or use the comparison built into the Overview, which does the work on screen."
  - question: "Is the PDF presentable to a client?"
    answer: "Yes, that is what it is for: structured, readable, free of unnecessary jargon. The CSV, on the other hand, is made to be sorted and filtered, not sent."
---

Two formats, two uses you should not mix up: the **CSV** is for sorting and filtering, the
**PDF** is for sharing.

No export uses up any quota: everything is built from data that has already been collected.

## A project's exports

### Summary report — CSV and PDF

The state of the site for the selected crawl: scores by category group, page counts, main
issues with their volume.

This is the document to send to someone who is not going to open the app — a client, an
executive, a contractor. Choose the PDF.

### Issue list — CSV and PDF

Every issue detected, with its type, its severity, its category, the number of URLs
affected, and the estimated priority and effort.

The CSV is the right choice here: in a spreadsheet, you sort by volume, you filter by severity,
and you get your work plan in thirty seconds.

### URLs per issue — CSV and PDF

For **one** given issue, the full list of affected URLs with the value found
on each — the duplicate `title`, the `canonical` pointing elsewhere, the missing `alt`.

This is the export to give to the person who will fix things by hand. It contains exactly what
is needed and nothing else.

### All URLs, all issues — CSV

The exhaustive export: every URL / issue pair on its own row.

Large by nature, and meant to be processed by a tool, not read. Useful for
matching against your own data — a CMS export, a list of priority pages.

### Fix pack — ZIP

The archive of ready-to-apply fixes, with instructions for each platform. It is the
main route when the site has no connected Git repository.

→ [Fix pack without a repository](/docs/fix-pack-sans-depot)

## Choose based on the recipient

| Recipient | Format | What they will do with it |
| --- | --- | --- |
| Client, executive | Summary report **PDF** | Read it, understand the state of the site, decide on a budget |
| Developer | URLs per issue **CSV** | Fix things, row by row |
| You, for planning | Issue list **CSV** | Sort, filter, prioritize |
| A site without a repository | **Fix pack ZIP** | Apply the fixes by hand |
| An external analysis | All URLs **CSV** | Cross-reference with other data |

## What a report you hand over should contain

A raw export is not a report. If you are billing for this audit, add three things that
{{app_name}} cannot guess:

1. **A summary in a few lines** — the overall state and the one number that matters.
2. **What you recommend doing, in order**, with the estimated effort. The product
   gives you the volume and the severity; the business trade-offs are yours to make.
3. **What has been fixed since the previous report**, with the proof — that is,
   the comparison between two crawls.

That third part is the one that justifies a recurring subscription for a client.
It is also the only one that competing tools do not produce on their own.

→ [Read the report](/docs/lire-le-rapport)
