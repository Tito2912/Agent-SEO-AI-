---
title: "Run an analysis and choose its scope"
meta_title: "SEO crawl: run and configure it — {{app_name}} documentation"
description: "Choose the pages and subdomains to analyze, adjust the available settings and follow the preparation of your report."
kind: "Documentation"
section: "Getting started"
order: 12
updated_at: "2026-10-21"
audience: "All plans"
keywords: ["SEO crawl", "crawl settings", "robots.txt", "depth"]
app_href: "/"
related: ["lire-le-rapport", "anomalies-et-priorites", "automatiser-les-audits", "plans-et-quotas"]
faq:
  - question: "My crawl stopped before the end. Why?"
    answer: "Three possible causes: the plan's or the project's page limit was reached, the maximum analysis duration expired, or the host started refusing requests. Open the tracking page in Activity to read the available information."
  - question: "Does {{app_name}} run JavaScript?"
    answer: "Yes, the crawler has a Chromium rendering engine. Rendering is slower and more expensive than reading the raw HTML, so it is kept for the cases where it changes the result."
  - question: "Does the crawl count images and CSS files in my page quota?"
    answer: "No. The quota counts the HTML pages crawled. Resources are only counted if you turn on their check, and they have a separate cap."
  - question: "Can I crawl a site I don't own?"
    answer: "To audit your own site, of course. For a third-party site, the Competitors feature exists and is deliberately limited to a sample of pages, at a reduced pace."
---

On the project page, start a **new analysis**. For a first pass, don't change anything: the
default values cover almost every site.

The crawl runs on a worker separate from the interface. Closing the tab does not stop it.

## What the crawler collects

For each page it reaches:

- the **HTTP status** and the redirect chain that leads to it;
- **indexability**: `robots.txt`, `meta robots`, `X-Robots-Tag` header, `canonical`;
- the **tags**: `title`, `meta description`, `h1` and the heading hierarchy, `hreflang`,
  `lang`, Open Graph, structured data;
- internal and external **links**, their anchor text, their `rel` attribute, and the click
  depth from the start page;
- **images**: `alt`, weight, format;
- whether the page is in the **XML sitemap**, and the reverse — the sitemap URLs that internal
  linking never reaches.

The result: a report, scores, and a list of typed issues.

## The settings that really matter

In the site menu, open **Analysis settings**. The number of pages and subdomains come first. The other choices are grouped in collapsible sections; on a shared site, collaborators can view the analysis settings and its owner can save them.

### Number of pages and subdomains

Choose a maximum number of pages. The engine applies your plan's caps and the monthly balance of the account that manages the site. The subdomains option widens the scope, for example to `blog.example.com`.

**Saving the settings does not start an analysis** and does not change any page on the site. Your choices are used for the next requests.

### Adjust the crawl

In **Adjust the crawl (advanced)**:

- **Simultaneous connections**: the number of pages requested in parallel. Lower it if your host refuses requests.
- **Maximum wait per page**: the time a page is given to respond.
- **Include / exclude addresses**: regular expressions to limit the analysis to one section or to leave out some addresses. These filters do not change what Google can crawl.
- **Ignore robots.txt**: an option to keep for a site you manage, with an understanding of the restrictions it sets.

The **Advanced options** group contains the crawler identifier (User-Agent).

### Additional data

Resources (images, styles and scripts), PageSpeed, Google Search Console and Bing are grouped under **Extra data**. Check the saved choices before changing them; Google and Bing permissions can also be managed from the site's connections. These services are optional to get a first analysis.

## PageSpeed and Core Web Vitals

The **Core Web Vitals** module, under **Extra data**: the device tested (mobile or desktop), the
number of pages to test, the maximum PageSpeed wait and the number of simultaneous requests.

This measurement goes through the Google PageSpeed Insights API, which is slow and rate-limited.
That is why it only applies to a sample of URLs, whose size depends on your plan.
The settings let you choose the maximum number of pages and the type of device; they do not let you pick each tested address by hand.

→ [Track performance](/docs/suivre-la-performance)

## Search Console and Bing in the same crawl

If you have connected Search Console to the project, the crawl adds the real search data:
impressions, clicks, position, queries. Same principle for Bing Webmaster Tools.

This combination is what turns the report from "here are the issues" into "here are the
issues on the pages that bring you traffic".

→ [Connect Search Console](/docs/connecter-search-console)

## Follow an analysis and find its report

Open **Activity**. The list gathers the recent tasks and the ones still running that your account can access. The Site, Type and Status filters apply to this list; use **Reload the list** to fetch other requests.

**View tracking** tells apart waiting, running, preparing the report, failure and cancellation. A "Completed" task is not enough to guarantee that its report can be read: the **View this analysis report** button appears when that specific report is available.

**Request a stop** acts on a running task. Once it is stopped or has failed, **Request a new analysis** creates a new request, subject to the limits and quota of the owner account. A stop does not guarantee that a partial report is available; previous reports remain available from the site.

If updating stops, **Refresh status** reads the tracking again without starting a new analysis. An expired session offers to log in again and come back to the same screen. The logs are available under **Technical details and logs**.
