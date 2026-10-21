# Data Processing Agreement (DPA)

> **DRAFT — NOT PUBLISHED, NOT OPERATIONAL.** This document is neither shown on the website nor
> offered to customers. It can only be once it has been legally validated and the items marked
> “TO BE COMPLETED” have been filled in (see `LEGAL-VALIDATION-REQUIRED.md`). Acceptance method
> chosen by the owner on 21/10/2026: a checkbox in the service. French version: `DPA-fr.md` (same
> clauses).

Article 28 of Regulation (EU) 2016/679 (“GDPR”).

## 1. Parties

- **The Customer**: the legal or natural person acting in a business capacity who uses the Noyaru
  service and who, for the processing described in Annex 1, is the **data controller**.
- **E-Com Shop**, SASU, 60 rue François 1er, 75008 Paris, France, 934 934 308 R.C.S. Paris,
  publisher of the Noyaru service, which acts as **data processor** for this processing.

This agreement supplements the Noyaru Terms and Conditions (noyaru.com/en/terms). In the event of
a conflict regarding the protection of personal data, this agreement prevails.

The Customer accepts this agreement online, by ticking the box provided for that purpose in the
service. E-Com Shop keeps a record of the account that accepted it, the date and the version
accepted. **TO BE COMPLETED**: where the box appears, and how a new version is accepted.

## 2. Subject matter and duration

This agreement governs the processing of personal data that E-Com Shop carries out on the
Customer's behalf when the Customer uses the service. It applies for as long as the Customer uses
the service, and then until the deletion of the data provided for in section 15.

It does not apply to the processing for which E-Com Shop is itself the data controller (account,
subscription, billing, support, security), described in the Privacy Policy (noyaru.com/en/privacy).

## 3. Nature of the processing operations

Collection by crawling the public pages of the sites indicated by the Customer; reading, with the
Customer's authorization, its code repository, its WordPress site and its search data (Google
Search Console, Bing Webmaster Tools); recording, analysis, transmission to artificial
intelligence models to produce fixes and content; writing changes through pull requests or in
WordPress when the Customer asks for it; storage; deletion.

## 4. Purposes

Providing the Customer with the features of the service it uses: technical audit of its sites,
suggesting and applying fixes, writing content, tracking search performance, competitor
analysis, assistant.

## 5. Categories of data

Any personal data contained in the items the Customer has processed: content of the pages of its
sites (names, business contact details, published texts), content of its code repository and of
its WordPress site, search queries appearing in its Search Console or Bing data, questions asked
to the assistant. Not all of this data is necessarily personal data.

The service is not designed to process special categories of data (Article 9 GDPR); the Customer
undertakes not to entrust any such data to it without prior written agreement.

## 6. Categories of data subjects

Persons whose data appears on the Customer's sites, in its repository or in its search data: for
example its employees, authors, customers or contacts, and the internet users whose queries
appear in the search statistics.

## 7. Documented instructions of the Customer

E-Com Shop processes the data only on the Customer's documented instructions. These instructions
are: this agreement, the Terms and Conditions, and the Customer's actions in the service (adding a
site, connecting a repository or a WordPress site, starting an analysis, requesting a fix,
enabling automatic merging).

E-Com Shop informs the Customer if, in its opinion, an instruction infringes the GDPR.

## 8. Confidentiality

E-Com Shop ensures that the persons authorized to process the data are bound by confidentiality.
**TO BE COMPLETED**: specify who, at E-Com Shop, has access to the data and under which
confidentiality commitment.

## 9. Security

E-Com Shop implements the measures described in Annex 2. They are limited to those the service
actually applies; they may be supplemented.

## 10. Subprocessors

The Customer authorizes E-Com Shop to use the subprocessors listed in Annex 3. E-Com Shop informs
the Customer of any intended addition or replacement; the Customer may object to it.
**TO BE COMPLETED**: means and timeframe of the notice, and consequence of an objection.

E-Com Shop imposes on each subprocessor data protection obligations equivalent to those of this
agreement.

## 11. International Data Transfers

Several subprocessors are established outside the European Union, in particular in the United
States (Annex 3). Depending on the subprocessor, these transfers rely on an adequacy decision of
the European Commission (including the EU–US Data Privacy Framework for certified companies) or on
the standard contractual clauses adopted by the Commission, supplemented where necessary by
additional measures. **TO BE COMPLETED**: the mechanism used for each subprocessor, with
supporting evidence.

## 12. Assistance with data subject requests

E-Com Shop assists the Customer, as far as possible, in responding to requests from data subjects
exercising their rights. The Customer can itself delete its projects and their results from the
service. Any request received directly by E-Com Shop is forwarded to the Customer.

## 13. Personal data breaches

E-Com Shop notifies the Customer of any personal data breach without undue delay after becoming
aware of it, with the information available to it on its nature, its likely consequences and the
measures taken. **TO BE COMPLETED**: contact point and notification channel.

## 14. Regulatory assistance

E-Com Shop assists the Customer, to the extent of the information available to it, with data
protection impact assessments and prior consultation of the supervisory authority (Articles 35
and 36 GDPR).

## 15. Deletion and return

When the Customer stops using the service, or deletes a project or its account, the data
processed for the Customer is erased from the live database; backups that still contain it
disappear at the end of their 30-day retention period. Before deletion, the Customer can retrieve
its reports through the exports of the service. Changes already written to its repository or its
site remain with the Customer.

## 16. Audits and compliance information

E-Com Shop makes available to the Customer the information necessary to demonstrate compliance
with this agreement and allows for audits, including inspections. **TO BE COMPLETED**: terms
(notice, frequency, costs, auditor bound by confidentiality).

## 17. Respective obligations

- **The Customer**: has a legal basis for the processing it entrusts, informs the data subjects,
  only has sites, repositories and data analyzed that it is authorized to process, and checks
  what is published on its sites.
- **E-Com Shop**: follows the Customer's instructions, Article 28 GDPR and this agreement.

## 18. Annexes

### Annex 1 — Description of the processing

| Item | Description |
| --- | --- |
| Subject matter | Providing the Noyaru features used by the Customer |
| Duration | Use of the service, then deletion (section 15) |
| Nature | Crawling, reading with authorization, analysis, transmission to AI models, writing on request, storage, deletion |
| Data | See section 5 |
| Data subjects | See section 6 |

### Annex 2 — Security measures applied by the service

- Encrypted connections (HTTPS).
- Account passwords stored only as hashes (PBKDF2).
- Tokens, keys and application passwords entrusted by the Customer encrypted at rest.
- Protection of forms against request forgery.
- Rate limiting to prevent abuse.
- Audit log of sensitive actions.
- Daily backups of the database, kept for 30 days.
- By default, no change is merged into the Customer's repository without its approval.

**TO BE COMPLETED**: internal access control, incident management, security testing, if they
exist.

### Annex 3 — Subprocessors

| Subprocessor | Role | Location |
| --- | --- | --- |
| Render Services, Inc. | Hosting of the application and the database | United States |
| Amazon Web Services | Backups and, where applicable, storage of reports | **TO BE COMPLETED** (region stated on the website: United States, Northern Virginia) |
| Anthropic | Artificial intelligence models | United States |
| OpenAI | Artificial intelligence models, as a backup | United States |
| Google (Gemini) | Artificial intelligence model for the assistant, if enabled | United States |
| Sentry | Tracking technical errors | **TO BE COMPLETED** (region stated on the website: European Union, Germany) |
| SerpAPI | Google searches for competitors and link opportunities | United States |
| Ahrefs | Backlink data, if the integration is enabled | Singapore |

The Customer's own services (GitHub, WordPress, Google Search Console, Bing Webmaster Tools,
Netlify) are not subprocessors: E-Com Shop accesses them with the Customer's authorization and on
its behalf. **TO BE CHECKED** by legal counsel.
