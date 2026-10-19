# Inventaire des écrans et interactions Noyaru

Généré par `python seo-agent-web/ops/ux_inventory.py`. Base de l’audit : `44f575c` (main, 19 octobre 2026).

193 déclarations de routes ; 50 templates directement référencés par les routes ; 69 templates HTML au total (inclusions et anciens templates compris).

Les verbes sont ceux déclarés dans le code. Une route peut servir plusieurs états ou rediriger. Les droits sont déterminés par les middlewares et les contrôles de projet/compte, pas par cette extraction. Voir [l’audit](UX_AUDIT_2026-10-19.md) pour les accès, constats et preuves navigateur.

## Routes, écrans, actions et sorties

Les API, exports, callbacks et actions POST sont inclus pour rendre visibles les étapes intermédiaires. Les fichiers statiques montés et les routes implicites du framework ne sont pas des écrans.

| Route | Méthode | Template(s) | Source et fonction |
| --- | --- | --- | --- |
| `/` | GET, HEAD | home_public.html, projects.html | [backend/app.py:14663](../backend/app.py#L14663) · `projects` |
| `/api/assistant/chat` | POST | — | [backend/app.py:16245](../backend/app.py#L16245) · `assistant_chat` |
| `/api/assistant/meta` | GET | — | [backend/app.py:16235](../backend/app.py#L16235) · `assistant_meta` |
| `/api/automation/domains` | GET | — | [backend/app.py:17801](../backend/app.py#L17801) · `api_automation_domains_get` |
| `/api/automation/domains` | POST | — | [backend/app.py:17815](../backend/app.py#L17815) · `api_automation_domains_save` |
| `/api/automation/github-corrections` | GET | — | [backend/app.py:17844](../backend/app.py#L17844) · `api_automation_github_corrections` |
| `/api/bing/sites` | GET | — | [backend/app.py:17536](../backend/app.py#L17536) · `bing_sites` |
| `/api/github/repos` | GET | — | [backend/app.py:16703](../backend/app.py#L16703) · `github_repos` |
| `/api/gsc/properties` | GET | — | [backend/app.py:17306](../backend/app.py#L17306) · `gsc_properties` |
| `/api/gsc/properties/via-oauth` | GET | — | [backend/app.py:17416](../backend/app.py#L17416) · `gsc_properties_via_oauth` |
| `/api/jobs/{job_id}` | GET | — | [backend/app.py:37131](../backend/app.py#L37131) · `job_api` |
| `/api/netlify/sites` | GET | — | [backend/app.py:16875](../backend/app.py#L16875) · `netlify_sites` |
| `/api/projects/{slug}/backlinks/auto-settings` | POST | — | [backend/app.py:36558](../backend/app.py#L36558) · `backlinks_auto_settings_save` |
| `/api/projects/{slug}/backlinks/generate-reply` | POST | — | [backend/app.py:36348](../backend/app.py#L36348) · `api_backlinks_generate_reply` |
| `/api/projects/{slug}/backlinks/queue/{opp_id}/approve` | POST | — | [backend/app.py:36603](../backend/app.py#L36603) · `backlinks_queue_approve` |
| `/api/projects/{slug}/backlinks/queue/{opp_id}/mark-sent` | POST | — | [backend/app.py:36639](../backend/app.py#L36639) · `backlinks_queue_mark_sent` |
| `/api/projects/{slug}/backlinks/queue/{opp_id}/reject` | POST | — | [backend/app.py:36621](../backend/app.py#L36621) · `backlinks_queue_reject` |
| `/api/projects/{slug}/backlinks/search` | POST | — | [backend/app.py:36279](../backend/app.py#L36279) · `api_backlinks_search` |
| `/api/projects/{slug}/bing/sites` | GET | — | [backend/app.py:17216](../backend/app.py#L17216) · `bing_sites_for_project` |
| `/api/projects/{slug}/competitors/etat` | GET | — | [backend/app.py:34734](../backend/app.py#L34734) · `api_competitors_etat` |
| `/api/projects/{slug}/competitors/suggest` | POST | — | [backend/app.py:34676](../backend/app.py#L34676) · `api_competitors_suggest` |
| `/api/projects/{slug}/content/article` | POST | — | [backend/app.py:34018](../backend/app.py#L34018) · `api_content_article` |
| `/api/projects/{slug}/content/article/{task_id}` | GET | — | [backend/app.py:34190](../backend/app.py#L34190) · `api_content_article_lire` |
| `/api/projects/{slug}/content/auto` | POST | — | [backend/app.py:35248](../backend/app.py#L35248) · `api_content_auto_settings` |
| `/api/projects/{slug}/content/draft` | POST | — | [backend/app.py:33504](../backend/app.py#L33504) · `api_content_draft` |
| `/api/projects/{slug}/content/langues` | GET | — | [backend/app.py:33577](../backend/app.py#L33577) · `api_content_langues` |
| `/api/projects/{slug}/content/suivi/{jeton}` | GET | — | [backend/app.py:32802](../backend/app.py#L32802) · `api_content_suivi` |
| `/api/projects/{slug}/content/sujet` | POST | — | [backend/app.py:34149](../backend/app.py#L34149) · `api_content_sujet` |
| `/api/projects/{slug}/github/bulk-fix` | POST | — | [backend/app.py:19363](../backend/app.py#L19363) · `api_github_bulk_fix` |
| `/api/projects/{slug}/github/connect` | POST | — | [backend/app.py:19084](../backend/app.py#L19084) · `api_github_connect` |
| `/api/projects/{slug}/github/status` | GET | — | [backend/app.py:19120](../backend/app.py#L19120) · `api_github_status` |
| `/api/projects/{slug}/gsc/properties` | GET | — | [backend/app.py:17096](../backend/app.py#L17096) · `gsc_properties_for_project` |
| `/api/projects/{slug}/issues/{issue_key}/deep-fix` | POST | — | [backend/app.py:30697](../backend/app.py#L30697) · `api_issue_deep_fix` |
| `/api/projects/{slug}/issues/{issue_key}/github-fix` | POST | — | [backend/app.py:19128](../backend/app.py#L19128) · `api_github_fix` |
| `/api/projects/{slug}/issues/{issue_key}/task` | POST | — | [backend/app.py:30943](../backend/app.py#L30943) · `api_issue_task_upsert` |
| `/api/projects/{slug}/issues/{issue_key}/url-fix` | GET | — | [backend/app.py:19019](../backend/app.py#L19019) · `api_issue_url_fix` |
| `/api/projects/{slug}/keywords/rewrite-pr` | POST | — | [backend/app.py:32572](../backend/app.py#L32572) · `api_keyword_rewrite_pr` |
| `/api/projects/{slug}/search-items` | GET | — | [backend/app.py:17660](../backend/app.py#L17660) · `project_search_items` |
| `/api/projects/{slug}/search-series` | GET | — | [backend/app.py:17601](../backend/app.py#L17601) · `project_search_series` |
| `/api/projects/{slug}/tasks` | GET | — | [backend/app.py:30977](../backend/app.py#L30977) · `api_project_tasks` |
| `/api/settings/activation` | GET | — | [backend/activation_routes.py:166](../backend/activation_routes.py#L166) · `activation_api` |
| `/api/settings/activation/plans` | GET | — | [backend/activation_routes.py:72](../backend/activation_routes.py#L72) · `current_plan_counts` |
| `/api/settings/operations` | GET | — | [backend/app.py:15785](../backend/app.py#L15785) · `api_settings_operations` |
| `/apple-touch-icon.png` | GET, HEAD | — | [backend/app.py:10336](../backend/app.py#L10336) · `apple_touch_icon` |
| `/auth/forgot` | GET | auth_forgot.html | [backend/app.py:12433](../backend/app.py#L12433) · `auth_forgot` |
| `/auth/forgot` | POST | auth_forgot.html | [backend/app.py:12455](../backend/app.py#L12455) · `auth_forgot_submit` |
| `/auth/google/callback` | GET | — | [backend/app.py:12995](../backend/app.py#L12995) · `auth_google_callback` |
| `/auth/google/start` | POST | — | [backend/app.py:12908](../backend/app.py#L12908) · `auth_google_start` |
| `/auth/login` | GET | auth_login.html | [backend/app.py:13306](../backend/app.py#L13306) · `auth_login` |
| `/auth/login` | POST | auth_login.html | [backend/app.py:13335](../backend/app.py#L13335) · `auth_login_submit` |
| `/auth/logout` | POST | — | [backend/app.py:13705](../backend/app.py#L13705) · `auth_logout` |
| `/auth/reset` | GET | auth_reset.html | [backend/app.py:12569](../backend/app.py#L12569) · `auth_reset` |
| `/auth/reset` | POST | auth_reset.html | [backend/app.py:12601](../backend/app.py#L12601) · `auth_reset_submit` |
| `/auth/signup` | GET | auth_signup.html | [backend/app.py:13462](../backend/app.py#L13462) · `auth_signup` |
| `/auth/signup` | POST | — | [backend/app.py:13503](../backend/app.py#L13503) · `auth_signup_submit` |
| `/auth/verify` | GET | auth_verify.html | [backend/app.py:12804](../backend/app.py#L12804) · `auth_verify` |
| `/auth/verify/resend` | GET | auth_verify_resend.html | [backend/app.py:12681](../backend/app.py#L12681) · `auth_verify_resend` |
| `/auth/verify/resend` | POST | auth_verify_resend.html | [backend/app.py:12710](../backend/app.py#L12710) · `auth_verify_resend_submit` |
| `/automation` | GET | automation.html | [backend/app.py:17742](../backend/app.py#L17742) · `automation` |
| `/billing` | GET | billing.html | [backend/app.py:14246](../backend/app.py#L14246) · `billing_page` |
| `/billing/cancel-scheduled-change` | POST | — | [backend/app.py:14500](../backend/app.py#L14500) · `billing_cancel_scheduled_change` |
| `/billing/checkout` | POST | — | [backend/app.py:14389](../backend/app.py#L14389) · `billing_checkout` |
| `/billing/portal` | POST | — | [backend/app.py:14540](../backend/app.py#L14540) · `billing_portal` |
| `/cron/auto-content` | POST | — | [backend/app.py:35415](../backend/app.py#L35415) · `cron_auto_content` |
| `/cron/auto-post-backlinks` | GET | — | [backend/app.py:36949](../backend/app.py#L36949) · `cron_auto_post_backlinks` |
| `/cron/auto-search-backlinks` | GET | — | [backend/app.py:36772](../backend/app.py#L36772) · `cron_auto_search_backlinks` |
| `/cron/autopilot` | GET | — | [backend/app.py:17883](../backend/app.py#L17883) · `cron_autopilot` |
| `/cron/check-backlinks` | GET | — | [backend/app.py:36682](../backend/app.py#L36682) · `cron_check_backlinks` |
| `/cron/refresh-competitors` | POST | — | [backend/app.py:34862](../backend/app.py#L34862) · `cron_refresh_competitors` |
| `/cron/verify-pull-requests` | POST | — | [backend/app.py:37859](../backend/app.py#L37859) · `cron_verify_pull_requests` |
| `/docs` | GET, HEAD | docs_index.html | [backend/app.py:14106](../backend/app.py#L14106) · `docs_index_public` |
| `/docs/{slug}` | GET, HEAD | docs_article.html | [backend/app.py:14127](../backend/app.py#L14127) · `docs_article_public` |
| `/favicon.ico` | GET, HEAD | — | [backend/app.py:10331](../backend/app.py#L10331) · `favicon_ico` |
| `/file` | GET | file.html | [backend/app.py:17985](../backend/app.py#L17985) · `view_file` |
| `/healthz` | GET | — | [backend/app.py:11787](../backend/app.py#L11787) · `healthz` |
| `/jobs` | GET | jobs.html | [backend/app.py:17933](../backend/app.py#L17933) · `jobs` |
| `/jobs/{job_id}` | GET | job.html | [backend/app.py:36957](../backend/app.py#L36957) · `job_detail` |
| `/jobs/{job_id}/apply-corrections` | POST | — | [backend/app.py:37351](../backend/app.py#L37351) · `apply_corrections` |
| `/jobs/{job_id}/cancel` | POST | — | [backend/app.py:37056](../backend/app.py#L37056) · `job_cancel` |
| `/jobs/{job_id}/retry` | POST | — | [backend/app.py:37083](../backend/app.py#L37083) · `job_retry` |
| `/mentions-legales` | GET, HEAD | legal_notice_public.html | [backend/app.py:13989](../backend/app.py#L13989) · `legal_notice_public` |
| `/oauth/bing/callback` | GET | — | [backend/app.py:17000](../backend/app.py#L17000) · `bing_oauth_callback` |
| `/oauth/bing/connect` | GET | — | [backend/app.py:16960](../backend/app.py#L16960) · `bing_oauth_connect` |
| `/oauth/bing/disconnect` | POST | — | [backend/app.py:17079](../backend/app.py#L17079) · `bing_oauth_disconnect` |
| `/oauth/github/callback` | GET | — | [backend/app.py:16622](../backend/app.py#L16622) · `github_oauth_callback` |
| `/oauth/github/connect` | GET | — | [backend/app.py:16588](../backend/app.py#L16588) · `github_oauth_connect` |
| `/oauth/github/disconnect` | POST | — | [backend/app.py:16693](../backend/app.py#L16693) · `github_oauth_disconnect` |
| `/oauth/google/callback` | GET | — | [backend/app.py:16379](../backend/app.py#L16379) · `google_oauth_callback` |
| `/oauth/netlify/callback` | GET | oauth_fragment_callback.html | [backend/app.py:16789](../backend/app.py#L16789) · `netlify_oauth_callback_page` |
| `/oauth/netlify/callback/complete` | POST | — | [backend/app.py:16806](../backend/app.py#L16806) · `netlify_oauth_complete` |
| `/oauth/netlify/connect` | GET | — | [backend/app.py:16758](../backend/app.py#L16758) · `netlify_oauth_connect` |
| `/oauth/netlify/disconnect` | POST | — | [backend/app.py:16865](../backend/app.py#L16865) · `netlify_oauth_disconnect` |
| `/pricing` | GET, HEAD | pricing_public.html | [backend/app.py:13901](../backend/app.py#L13901) · `pricing_public` |
| `/privacy` | GET, HEAD | privacy_public.html | [backend/app.py:13958](../backend/app.py#L13958) · `privacy_public` |
| `/projects/add` | POST | — | [backend/app.py:16082](../backend/app.py#L16082) · `add_project` |
| `/projects/crawl-batch` | POST | — | [backend/app.py:18245](../backend/app.py#L18245) · `crawl_projects_batch` |
| `/projects/delete` | POST | — | [backend/app.py:16192](../backend/app.py#L16192) · `delete_projects` |
| `/projects/first-audit` | POST | — | [backend/first_audit.py:79](../backend/first_audit.py#L79) · `start` |
| `/projects/{slug}` | GET | project_overview.html | [backend/app.py:18451](../backend/app.py#L18451) · `project_overview` |
| `/projects/{slug}/automation` | GET | project_automation.html | [backend/app.py:30999](../backend/app.py#L30999) · `project_automation` |
| `/projects/{slug}/backlinks` | GET | backlinks.html | [backend/app.py:35566](../backend/app.py#L35566) · `project_backlinks` |
| `/projects/{slug}/backlinks/ahrefs/sync` | POST | — | [backend/app.py:35930](../backend/app.py#L35930) · `backlinks_ahrefs_sync` |
| `/projects/{slug}/backlinks/clear` | POST | — | [backend/app.py:35892](../backend/app.py#L35892) · `backlinks_clear` |
| `/projects/{slug}/backlinks/import` | POST | — | [backend/app.py:35801](../backend/app.py#L35801) · `backlinks_import` |
| `/projects/{slug}/backlinks/opportunities` | GET | backlinks_opportunities.html | [backend/app.py:36181](../backend/app.py#L36181) · `project_backlinks_opportunities` |
| `/projects/{slug}/backlinks/opportunities/save` | POST | — | [backend/app.py:36417](../backend/app.py#L36417) · `backlinks_opportunity_save` |
| `/projects/{slug}/backlinks/opportunities/{opp_id}/delete` | POST | — | [backend/app.py:36486](../backend/app.py#L36486) · `backlinks_opportunity_delete` |
| `/projects/{slug}/backlinks/opportunities/{opp_id}/status` | POST | — | [backend/app.py:36509](../backend/app.py#L36509) · `backlinks_opportunity_status` |
| `/projects/{slug}/cms` | GET | cms_settings.html | [backend/cms_routes.py:131](../backend/cms_routes.py#L131) · `cms_settings` |
| `/projects/{slug}/cms/platform` | POST | — | [backend/cms_routes.py:147](../backend/cms_routes.py#L147) · `cms_platform` |
| `/projects/{slug}/cms/wordpress/changes/{change_id}` | GET | cms_wordpress_change.html | [backend/cms_routes.py:337](../backend/cms_routes.py#L337) · `wordpress_change` |
| `/projects/{slug}/cms/wordpress/changes/{change_id}/status` | GET | — | [backend/cms_routes.py:443](../backend/cms_routes.py#L443) · `wordpress_change_status` |
| `/projects/{slug}/cms/wordpress/changes/{change_id}/{action}` | POST | — | [backend/cms_routes.py:547](../backend/cms_routes.py#L547) · `wordpress_change_action` |
| `/projects/{slug}/cms/wordpress/connect` | POST | — | [backend/cms_routes.py:168](../backend/cms_routes.py#L168) · `wordpress_connect` |
| `/projects/{slug}/cms/wordpress/disconnect` | POST | — | [backend/cms_routes.py:221](../backend/cms_routes.py#L221) · `wordpress_disconnect` |
| `/projects/{slug}/cms/wordpress/edit` | GET | cms_wordpress_edit.html | [backend/cms_routes.py:234](../backend/cms_routes.py#L234) · `wordpress_edit` |
| `/projects/{slug}/cms/wordpress/issues/{issue_key}` | GET | cms_wordpress_batch.html | [backend/cms_routes.py:689](../backend/cms_routes.py#L689) · `wordpress_batch` |
| `/projects/{slug}/cms/wordpress/issues/{issue_key}/apply` | POST | — | [backend/cms_routes.py:737](../backend/cms_routes.py#L737) · `wordpress_batch_apply` |
| `/projects/{slug}/cms/wordpress/issues/{issue_key}/propose` | POST | — | [backend/cms_routes.py:716](../backend/cms_routes.py#L716) · `wordpress_batch_propose` |
| `/projects/{slug}/cms/wordpress/issues/{issue_key}/verify` | POST | — | [backend/cms_routes.py:790](../backend/cms_routes.py#L790) · `wordpress_batch_verify` |
| `/projects/{slug}/cms/wordpress/preview` | POST | — | [backend/cms_routes.py:259](../backend/cms_routes.py#L259) · `wordpress_preview` |
| `/projects/{slug}/cms/wordpress/suggest` | POST | — | [backend/cms_routes.py:308](../backend/cms_routes.py#L308) · `wordpress_suggest` |
| `/projects/{slug}/competitors` | GET | competitors.html | [backend/app.py:34508](../backend/app.py#L34508) · `project_competitors` |
| `/projects/{slug}/competitors/add` | POST | — | [backend/app.py:34759](../backend/app.py#L34759) · `project_competitor_add` |
| `/projects/{slug}/competitors/analyze` | POST | — | [backend/app.py:34818](../backend/app.py#L34818) · `project_competitor_analyze` |
| `/projects/{slug}/competitors/remove` | POST | — | [backend/app.py:34800](../backend/app.py#L34800) · `project_competitor_remove` |
| `/projects/{slug}/content` | GET | content.html | [backend/app.py:34336](../backend/app.py#L34336) · `project_content` |
| `/projects/{slug}/content/valider/{task_id}` | GET | content_valider.html | [backend/app.py:35335](../backend/app.py#L35335) · `page_contenu_valider` |
| `/projects/{slug}/content/valider/{task_id}` | POST | — | [backend/app.py:35355](../backend/app.py#L35355) · `page_contenu_valider_post` |
| `/projects/{slug}/corrections` | GET | corrections.html | [backend/app.py:31196](../backend/app.py#L31196) · `project_corrections` |
| `/projects/{slug}/crawl` | POST | — | [backend/app.py:18106](../backend/app.py#L18106) · `crawl_project` |
| `/projects/{slug}/crawls` | GET | crawls.html | [backend/app.py:31907](../backend/app.py#L31907) · `project_crawls` |
| `/projects/{slug}/export/fix-pack.zip` | GET | — | [backend/app.py:31443](../backend/app.py#L31443) · `export_project_fix_pack_zip` |
| `/projects/{slug}/export/issues-all-urls.csv` | GET | — | [backend/app.py:31556](../backend/app.py#L31556) · `export_project_issues_all_urls_csv` |
| `/projects/{slug}/export/issues.csv` | GET | — | [backend/app.py:31487](../backend/app.py#L31487) · `export_project_issues_csv` |
| `/projects/{slug}/export/issues.pdf` | GET | — | [backend/app.py:31679](../backend/app.py#L31679) · `export_project_issues_pdf` |
| `/projects/{slug}/export/issues/{issue_key}.csv` | GET | — | [backend/app.py:31744](../backend/app.py#L31744) · `export_project_issue_csv` |
| `/projects/{slug}/export/issues/{issue_key}.pdf` | GET | — | [backend/app.py:31837](../backend/app.py#L31837) · `export_project_issue_pdf` |
| `/projects/{slug}/export/report.csv` | GET | — | [backend/app.py:31361](../backend/app.py#L31361) · `export_project_report_csv` |
| `/projects/{slug}/export/report.pdf` | GET | — | [backend/app.py:31400](../backend/app.py#L31400) · `export_project_report_pdf` |
| `/projects/{slug}/first-correction` | GET | first_correction.html | [backend/app.py:31107](../backend/app.py#L31107) · `project_first_correction` |
| `/projects/{slug}/first-correction` | POST | — | [backend/app.py:31120](../backend/app.py#L31120) · `project_first_correction_update` |
| `/projects/{slug}/fix-suggestions/generate` | POST | — | [backend/app.py:18892](../backend/app.py#L18892) · `project_generate_fix_suggestions` |
| `/projects/{slug}/gsc/oauth/connect` | GET | — | [backend/app.py:16340](../backend/app.py#L16340) · `project_gsc_oauth_connect` |
| `/projects/{slug}/gsc/oauth/disconnect` | POST | — | [backend/app.py:16541](../backend/app.py#L16541) · `project_gsc_oauth_disconnect` |
| `/projects/{slug}/issues` | GET | issues.html | [backend/app.py:18843](../backend/app.py#L18843) · `project_issues` |
| `/projects/{slug}/issues/{issue_key}` | GET | issue_detail.html | [backend/app.py:18927](../backend/app.py#L18927) · `project_issue_detail` |
| `/projects/{slug}/keywords` | GET | performance.html | [backend/app.py:35435](../backend/app.py#L35435) · `project_performance` |
| `/projects/{slug}/keywords/opportunities` | GET | keyword_opportunities.html | [backend/app.py:32412](../backend/app.py#L32412) · `project_keyword_opportunities` |
| `/projects/{slug}/keywords/track` | POST | — | [backend/app.py:32486](../backend/app.py#L32486) · `project_keyword_track` |
| `/projects/{slug}/keywords/untrack` | POST | — | [backend/app.py:32532](../backend/app.py#L32532) · `project_keyword_untrack` |
| `/projects/{slug}/netlinking` | GET | backlinks.html | [backend/app.py:35566](../backend/app.py#L35566) · `project_backlinks` |
| `/projects/{slug}/performance` | GET | performance.html | [backend/app.py:35435](../backend/app.py#L35435) · `project_performance` |
| `/projects/{slug}/search-connections/{source}` | POST | — | [backend/app.py:18417](../backend/app.py#L18417) · `project_search_connection_update` |
| `/projects/{slug}/settings/crawl` | GET | crawl_settings.html | [backend/app.py:18647](../backend/app.py#L18647) · `project_crawl_settings` |
| `/projects/{slug}/settings/crawl` | POST | — | [backend/app.py:18723](../backend/app.py#L18723) · `project_crawl_settings_save` |
| `/ressources-seo` | GET, HEAD | seo_resources_public.html | [backend/app.py:14151](../backend/app.py#L14151) · `seo_resources_public` |
| `/ressources-seo/{slug}` | GET, HEAD | seo_resource_article_public.html | [backend/app.py:14168](../backend/app.py#L14168) · `seo_resource_article_public` |
| `/robots.txt` | GET, HEAD | — | [backend/app.py:14188](../backend/app.py#L14188) · `robots_txt` |
| `/run` | POST | — | [backend/app.py:18027](../backend/app.py#L18027) · `run` |
| `/settings` | GET | — | [backend/app.py:14809](../backend/app.py#L14809) · `settings_root` |
| `/settings/account/attribution/withdraw` | POST | — | [backend/activation_routes.py:219](../backend/activation_routes.py#L219) · `withdraw_attribution` |
| `/settings/account/country` | POST | — | [backend/activation_routes.py:109](../backend/activation_routes.py#L109) · `save_own_country` |
| `/settings/account/delete` | POST | — | [backend/app.py:14999](../backend/app.py#L14999) · `settings_account_delete` |
| `/settings/account/password` | POST | — | [backend/app.py:15138](../backend/app.py#L15138) · `settings_account_password` |
| `/settings/account/preferences` | POST | — | [backend/app.py:15205](../backend/app.py#L15205) · `settings_account_preferences_save` |
| `/settings/accounts` | GET | settings_accounts.html | [backend/app.py:14839](../backend/app.py#L14839) · `settings_accounts` |
| `/settings/accounts` | POST | — | [backend/app.py:14955](../backend/app.py#L14955) · `settings_accounts_save` |
| `/settings/accounts/gsc/disconnect-all` | POST | — | [backend/app.py:16565](../backend/app.py#L16565) · `gsc_disconnect_all_projects` |
| `/settings/activation` | GET | settings_activation.html | [backend/activation_routes.py:47](../backend/activation_routes.py#L47) · `activation_page` |
| `/settings/activation.csv` | GET | — | [backend/activation_routes.py:173](../backend/activation_routes.py#L173) · `activation_csv` |
| `/settings/activation/accounts` | GET | settings_customer_accounts.html | [backend/activation_routes.py:98](../backend/activation_routes.py#L98) · `accounts_page` |
| `/settings/activation/accounts.csv` | GET | — | [backend/activation_routes.py:127](../backend/activation_routes.py#L127) · `accounts_csv` |
| `/settings/activation/accounts/{account_id}` | GET | settings_customer_account_profile.html | [backend/activation_routes.py:158](../backend/activation_routes.py#L158) · `account_profile_page` |
| `/settings/activation/accounts/{account_id}/export.csv` | GET | — | [backend/activation_routes.py:149](../backend/activation_routes.py#L149) · `account_profile_csv` |
| `/settings/activation/acquisition.csv` | GET | — | [backend/activation_routes.py:189](../backend/activation_routes.py#L189) · `acquisition_csv` |
| `/settings/activation/campaigns` | POST | — | [backend/activation_routes.py:206](../backend/activation_routes.py#L206) · `create_campaign` |
| `/settings/operations` | GET | settings_operations.html | [backend/app.py:15766](../backend/app.py#L15766) · `settings_operations` |
| `/settings/system` | GET | settings_system.html | [backend/app.py:15791](../backend/app.py#L15791) · `settings_system` |
| `/settings/system` | POST | — | [backend/app.py:16023](../backend/app.py#L16023) · `settings_system_save` |
| `/settings/system/secrets/rotate` | POST | — | [backend/app.py:15983](../backend/app.py#L15983) · `settings_system_rotate_secrets` |
| `/settings/team` | GET | settings_team.html | [backend/app.py:37449](../backend/app.py#L37449) · `settings_team` |
| `/settings/team/invite` | POST | — | [backend/app.py:37503](../backend/app.py#L37503) · `settings_team_invite` |
| `/settings/team/invite/{invite_id}/revoke` | POST | — | [backend/app.py:37543](../backend/app.py#L37543) · `settings_team_invite_revoke` |
| `/settings/team/leave` | POST | — | [backend/app.py:37569](../backend/app.py#L37569) · `settings_team_leave` |
| `/settings/team/member/{member_id}/remove` | POST | — | [backend/app.py:37557](../backend/app.py#L37557) · `settings_team_member_remove` |
| `/site.webmanifest` | GET, HEAD | — | [backend/app.py:10341](../backend/app.py#L10341) · `site_webmanifest` |
| `/sitemap.xml` | GET, HEAD | — | [backend/app.py:14215](../backend/app.py#L14215) · `sitemap_xml` |
| `/status` | GET, HEAD | status_public.html | [backend/app.py:14028](../backend/app.py#L14028) · `status_public` |
| `/stripe/webhook` | POST | — | [backend/app.py:14616](../backend/app.py#L14616) · `stripe_webhook` |
| `/support` | GET, HEAD | support_public.html | [backend/app.py:14014](../backend/app.py#L14014) · `support_public` |
| `/team/accept` | GET | — | [backend/app.py:37594](../backend/app.py#L37594) · `team_accept` |
| `/terms` | GET, HEAD | terms_public.html | [backend/app.py:13919](../backend/app.py#L13919) · `terms_public` |

## Fenêtres, formulaires et panneaux par template

Extraction statique des balises `form`, `dialog`, `details` et des régions `role=dialog`. Les expressions Jinja sont conservées. Les boutons pilotés par JavaScript sont examinés dans le bilan ; cette liste n’atteste ni l’ouverture d’une fenêtre ni la soumission d’un formulaire.

### _capture.html

Inclusion, gabarit partagé ou ancien template sans référence directe de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### _cms_guidance.html

Inclusion, gabarit partagé ou ancien template sans référence directe de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### _cms_workflow.html

Inclusion, gabarit partagé ou ancien template sans référence directe de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### _comparatif.html

Inclusion, gabarit partagé ou ancien template sans référence directe de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### _content_styles.html

Inclusion, gabarit partagé ou ancien template sans référence directe de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### _country_field.html

Inclusion, gabarit partagé ou ancien template sans référence directe de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### _customer_follow_up.html

Inclusion, gabarit partagé ou ancien template sans référence directe de route.

- Ligne 3 : `<form method="get" action="/settings/activation#clients-a-accompagner">`
- Ligne 9 : `<details class="follow-up-method">`

### _favicons.html

Inclusion, gabarit partagé ou ancien template sans référence directe de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### _first_audit_start.html

Inclusion, gabarit partagé ou ancien template sans référence directe de route.

- Ligne 6 : `<form method="post" action="/projects/first-audit" data-audit-submit>`
- Ligne 24 : `<details class="first-audit-options">`

### _overview_history.html

Inclusion, gabarit partagé ou ancien template sans référence directe de route.

- Ligne 44 : `<details class="overview-history-records">`

### _search_connection_badge.html

Inclusion, gabarit partagé ou ancien template sans référence directe de route.

- Ligne 2 : `<details class="search-connection" id="{{ connection.source }}-connection">`
- Ligne 12 : `<form method="post" action="/projects/{{ slug }}/search-connections/{{ connection.source }}">`
- Ligne 26 : `<form method="post" action="/projects/{{ slug }}/search-connections/{{ connection.source }}">`

### _tarif_fonctions.html

Inclusion, gabarit partagé ou ancien template sans référence directe de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### _tarifs.html

Inclusion, gabarit partagé ou ancien template sans référence directe de route.

- Ligne 39 : `<details class="tarifs-detail">`

### _tarifs_styles.html

Inclusion, gabarit partagé ou ancien template sans référence directe de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### auth_forgot.html

Template de route.

- Ligne 36 : `<form method="post" action="/auth/forgot">`

### auth_login.html

Template de route.

- Ligne 40 : `<form method="post" action="/auth/login">`

### auth_reset.html

Template de route.

- Ligne 30 : `<form method="post" action="/auth/reset">`

### auth_signup.html

Template de route.

- Ligne 42 : `<form method="post" action="/auth/signup">`

### auth_verify.html

Template de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### auth_verify_resend.html

Template de route.

- Ligne 36 : `<form method="post" action="/auth/verify/resend">`

### automation.html

Template de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### backlinks.html

Template de route.

- Ligne 18 : `<form method="post" action="/projects/{{ project.slug }}/crawl" class="inline">`
- Ligne 47 : `<form method="get" class="filters">`
- Ligne 68 : `<form method="post" action="/projects/{{ project.slug }}/backlinks/import" enctype="multipart/form-data" class="filters" style="align-items:flex-end;" >`
- Ligne 126 : `<form method="post" action="/projects/{{ project.slug }}/backlinks/ahrefs/sync" class="inline">`
- Ligne 165 : `<details class="setting-details">`
- Ligne 252 : `<form method="post" action="/projects/{{ project.slug }}/backlinks/clear" style="margin-top:12px;">`

### backlinks_opportunities.html

Template de route.

- Ligne 247 : `<details>`
- Ligne 309 : `<form method="get" class="opp-filters">`
- Ligne 352 : `<form method="post" action="/projects/{{ slug }}/backlinks/opportunities/{{ opp.id }}/delete" onsubmit="return confirm('Supprimer ?');" style="margin:0;">`
- Ligne 367 : `<details style="margin-top:10px;">`
- Ligne 388 : `<details {% if auto_cfg.enabled %}open{% endif %}>`

### billing.html

Template de route.

- Ligne 65 : `<form method="post" action="/billing/portal" class="inline">`
- Ligne 71 : `<form method="post" action="/billing/portal" class="inline">`
- Ligne 186 : `<form method="post" action="/billing/cancel-scheduled-change" class="inline" style="margin-top:10px;">`
- Ligne 232 : `<form method="post" action="/billing/portal">`
- Ligne 238 : `<form method="post" action="/billing/checkout">`

### cms_settings.html

Template de route.

- Ligne 12 : `<form method="post" action="/projects/{{ slug }}/cms/platform" class="cms-form">`
- Ligne 26 : `<form method="post" action="/projects/{{ slug }}/cms/wordpress/disconnect">`
- Ligne 29 : `<details class="cms-help">`
- Ligne 30 : `<form method="post" action="/projects/{{ slug }}/cms/wordpress/connect" class="cms-form">`

### cms_wordpress_batch.html

Template de route.

- Ligne 41 : `<form method="post" action="/projects/{{ slug }}/cms/wordpress/issues/{{ issue_key }}/propose" class="inline">`
- Ligne 53 : `<form method="post" action="/projects/{{ slug }}/cms/wordpress/issues/{{ issue_key }}/apply" class="card" id="batch-review">`
- Ligne 85 : `<form method="post" action="/projects/{{ slug }}/cms/wordpress/issues/{{ issue_key }}/verify">`

### cms_wordpress_change.html

Template de route.

- Ligne 7 : `<form method="post" action="/projects/{{ slug }}/cms/wordpress/changes/{{ change.id }}/{{ action }}">`
- Ligne 59 : `<details>`
- Ligne 60 : `<details class="cms-help" id="restoration">`

### cms_wordpress_edit.html

Template de route.

- Ligne 14 : `<form method="post" action="/projects/{{ slug }}/cms/wordpress/suggest" class="cms-form" id="cms-suggest">`
- Ligne 20 : `<form method="post" action="/projects/{{ slug }}/cms/wordpress/preview" class="cms-form">`

### competitors.html

Template de route.

- Ligne 38 : `<form method="post" action="/projects/{{ slug }}/competitors/add" class="setting-form">`
- Ligne 160 : `<form method="post" action="/projects/{{ slug }}/competitors/analyze" class="inline">`
- Ligne 167 : `<form method="post" action="/projects/{{ slug }}/competitors/remove" class="inline">`

### content.html

Template de route.

- Ligne 75 : `<details class="screen-options content-style">`

### content_valider.html

Template de route.

- Ligne 31 : `<form method="post" action="/projects/{{ slug }}/content/valider/{{ task_id }}" class="modal-actions between">`

### corrections.html

Template de route.

- Ligne 627 : `<form method="post" action="/projects/{{ slug }}/crawl" class="inline">`

### crawl_settings.html

Template de route.

- Ligne 166 : `<form method="post" action="/projects/{{ slug }}/settings/crawl">`
- Ligne 629 : `<details class="cs-card">`

### crawls.html

Template de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### docs_article.html

Template de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### docs_index.html

Template de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### error.html

Inclusion, gabarit partagé ou ancien template sans référence directe de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### file.html

Template de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### first_correction.html

Template de route.

- Ligne 17 : `<form method="post" action="/projects/{{ slug }}/crawl">`
- Ligne 70 : `<form method="post" action="/projects/{{ slug }}/crawl">`
- Ligne 88 : `<form method="post" action="/projects/{{ slug }}/first-correction">`
- Ligne 128 : `<details class="cms-help">`
- Ligne 141 : `<form method="post" action="/projects/{{ slug }}/first-correction">`

### home_public.html

Template de route.

- Ligne 320 : `<details>`
- Ligne 324 : `<details>`
- Ligne 328 : `<details>`
- Ligne 332 : `<details>`
- Ligne 336 : `<details>`
- Ligne 340 : `<details>`

### index.html

Inclusion, gabarit partagé ou ancien template sans référence directe de route.

- Ligne 21 : `<form method="post" action="/run">`

### issue_detail.html

Template de route.

- Ligne 49 : `<details class="technical-details">`
- Ligne 87 : `<form method="post" action="/projects/{{ project.slug }}/fix-suggestions/generate" class="inline">`
- Ligne 207 : `<form method="get" class="filters" style="margin-top: 8px;">`
- Ligne 257 : `<details class="logs-details">`
- Ligne 293 : `<details class="logs-details">`

### issues.html

Template de route.

- Ligne 19 : `<form method="post" action="/projects/{{ project.slug }}/crawl" class="inline">`
- Ligne 84 : `<details class="screen-options">`
- Ligne 95 : `<form method="post" action="/projects/{{ project.slug }}/fix-suggestions/generate" class="inline">`
- Ligne 104 : `<details class="screen-options" {% if severity or category or q %}open{% endif %}>`
- Ligne 107 : `<form method="get" class="filters" style="width:100%;">`
- Ligne 173 : `<details class="technical-details">`
- Ligne 226 : `<details class="technical-details">`

### job.html

Template de route.

- Ligne 41 : `<form method="post" action="/jobs/{{ job.id }}/cancel" class="inline">`
- Ligne 47 : `<form method="post" action="/jobs/{{ job.id }}/retry" class="inline">`
- Ligne 144 : `<details class="card screen-options">`
- Ligne 190 : `<form action="/jobs/{{ job.id }}/apply-corrections" method="post">`

### jobs.html

Template de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### keyword_opportunities.html

Template de route.

- Ligne 82 : `<form method="post" action="/projects/{{ slug }}/keywords/track" class="inline">`
- Ligne 126 : `<form method="post" action="/projects/{{ slug }}/keywords/track" class="setting-form">`
- Ligne 170 : `<form method="post" action="/projects/{{ slug }}/keywords/untrack" class="inline">`

### keywords.html

Inclusion, gabarit partagé ou ancien template sans référence directe de route.

- Ligne 18 : `<form method="post" action="/projects/{{ project.slug }}/crawl" class="inline">`
- Ligne 35 : `<form method="get" class="filters">`

### layout.html

Inclusion, gabarit partagé ou ancien template sans référence directe de route.

- Ligne 67 : `<details class="nav-group" {% if '/performance' in request.url.path or '/keywords' in request.url.path or '/competitors' in request.url.path or '/backlinks' in request.url.path or '/netlinking' in request.url.path %}open{% endif %}>`
- Ligne 94 : `<details class="nav-group" {% if '/settings/crawl' in request.url.path or '/automation' in request.url.path or '/crawls' in request.url.path or '/cms' in request.url.path %}open{% endif %}>`
- Ligne 181 : `<form method="post" action="/auth/logout">`
- Ligne 228 : `<form class="assistant-form" id="assistant-form">`
- Ligne 262 : `<form action="/settings/account/country" method="post">`

### legal_notice_public.html

Template de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### oauth_fragment_callback.html

Template de route.

- Ligne 41 : `<form id="oauth-complete-form" method="post" action="{{ complete_url }}">`

### performance.html

Template de route.

- Ligne 22 : `<form method="post" action="/projects/{{ project.slug }}/crawl" class="inline">`
- Ligne 64 : `<details class="technical-details">`
- Ligne 67 : `<details class="screen-options" {% if q %}open{% endif %}>`
- Ligne 70 : `<form method="get" class="filters" style="width:100%;">`

### pricing_public.html

Template de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### privacy_public.html

Template de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### project_automation.html

Template de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### project_overview.html

Template de route.

- Ligne 24 : `<form method="post" action="/projects/{{ project.slug }}/crawl" class="inline">`
- Ligne 122 : `<details class="overview-score-help">`
- Ligne 160 : `<form method="post" action="/projects/{{ project.slug }}/crawl" class="inline">`
- Ligne 212 : `<details class="logs-details" id="live-job-logs-details" style="display:none;">`
- Ligne 241 : `<form method="post" action="/projects/{{ project.slug }}/crawl" class="inline">`
- Ligne 268 : `<details class="screen-options overview-report-controls">`
- Ligne 271 : `<form method="get" class="filters" style="width:100%;">`
- Ligne 338 : `<details class="overview-action-details">`
- Ligne 477 : `<details class="overview-cwv-details">`

### projects.html

Template de route.

- Ligne 51 : `<form method="post" action="{{ resume.href }}" data-audit-submit>`
- Ligne 137 : `<details class="screen-options optional-connections">`
- Ligne 168 : `<form id="batch-crawl-form" method="post" action="/projects/crawl-batch">`
- Ligne 171 : `<form id="delete-projects-form" method="post" action="/projects/delete">`
- Ligne 368 : `<form method="post" action="/projects/{{ p.slug }}/crawl" class="crawl-start-form" data-slug="{{ p.slug }}">`
- Ligne 588 : `<dialog id="add-project-dialog" class="modal">`
- Ligne 603 : `<form method="dialog">`
- Ligne 620 : `<form method="post" action="/projects/add">`
- Ligne 637 : `<form method="post" action="/projects/add">`
- Ligne 655 : `<form method="post" action="/projects/add" id="gsc-add-form">`

### public_layout.html

Inclusion, gabarit partagé ou ancien template sans référence directe de route.

- Ligne 103 : `<form method="post" action="/auth/logout" class="inline">`
- Ligne 132 : `<div id="cookie-banner" role="dialog" aria-label="Politique de cookies" style="display:none;position:fixed;bottom:0;left:0;right:0;z-index:9999;background:#111;color:#fff;padding:14px 20px;display:flex;align-items:center;gap:16px;flex-wrap:wrap;justify-content:space-between;font-size:14px;">`

### seo_resource_article_public.html

Template de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### seo_resources_public.html

Template de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### settings_accounts.html

Template de route.

- Ligne 7 : `<dialog id="info-{{ item.key }}" class="modal">`
- Ligne 10 : `<form method="dialog">`
- Ligne 83 : `<details class="setting-details" {% if item.source == "none" %}open{% endif %}>`
- Ligne 85 : `<form method="post" action="/settings/accounts" class="setting-form">`
- Ligne 201 : `<form method="post" action="/settings/accounts/gsc/disconnect-all" style="display:inline-flex;">`
- Ligne 229 : `<form method="post" action="{{ p.disconnect_url }}" style="display:contents;">`
- Ligne 349 : `<form method="post" action="{{ github_oauth.disconnect_url }}" style="display:inline-flex;">`
- Ligne 363 : `<details class="setting-details">`
- Ligne 365 : `<form method="post" action="/settings/accounts" class="setting-form">`
- Ligne 465 : `<form method="post" action="{{ bing_oauth.disconnect_url }}" style="display:inline-flex;">`
- Ligne 507 : `<details class="setting-details" {% if bing_oauth.source == "none" and not bing_oauth.ready %}open{% endif %}>`
- Ligne 509 : `<form method="post" action="/settings/accounts" class="setting-form">`
- Ligne 628 : `<form method="post" action="{{ netlify_oauth.disconnect_url }}" style="display:inline-flex;">`
- Ligne 642 : `<details class="setting-details">`
- Ligne 644 : `<form method="post" action="/settings/accounts" class="setting-form">`
- Ligne 704 : `<form method="post" action="/settings/account/attribution/withdraw">`
- Ligne 721 : `<form method="post" action="/settings/account/preferences" style="margin-top:16px;">`
- Ligne 828 : `<details class="setting-details">`
- Ligne 830 : `<form method="post" action="/settings/account/password" class="setting-form">`
- Ligne 865 : `<details class="setting-details">`
- Ligne 867 : `<form method="post" action="/settings/account/delete" class="setting-form">`
- Ligne 923 : `<form method="post" action="/projects/add" style="display:inline-flex; gap:8px; align-items:center;">`

### settings_activation.html

Template de route.

- Ligne 16 : `<form method="get" action="/settings/activation#active-plans" data-plan-refresh-form>`
- Ligne 35 : `<details class="live-plans-method">`
- Ligne 38 : `<form class="activation-period card" method="get" action="/settings/activation">`
- Ligne 71 : `<details class="card campaign-links" id="campaign-links" {% if not campaigns or err or msg %}open{% endif %}>`
- Ligne 74 : `<form method="post" action="/settings/activation/campaigns" class="campaign-form">`
- Ligne 86 : `<details class="card activation-method">`

### settings_customer_account_profile.html

Template de route.

- Ligne 42 : `<details class="profile-extra">`
- Ligne 46 : `<details>`

### settings_customer_accounts.html

Template de route.

- Ligne 17 : `<form class="card account-filters" action="/settings/activation/accounts" method="get">`
- Ligne 32 : `<details>`
- Ligne 45 : `<details class="card account-method">`

### settings_operations.html

Template de route.

- Ligne 53 : `<form method="get" action="/settings/operations" class="inline" style="display:flex;gap:6px;align-items:center;">`

### settings_system.html

Template de route.

- Ligne 43 : `<details class="setting-details">`
- Ligne 45 : `<form method="post" action="{{ action }}" class="setting-form">`
- Ligne 69 : `<dialog id="info-{{ item.key }}" class="modal">`
- Ligne 72 : `<form method="dialog">`
- Ligne 188 : `<form method="post" action="/settings/system/secrets/rotate" class="setting-form" style="margin-top:0;">`
- Ligne 246 : `<details class="setting-details" {% if not (cred and cred.exists) %}open{% endif %}>`
- Ligne 248 : `<form method="post" action="/settings/system" id="gsc-form" class="setting-form">`

### settings_team.html

Template de route.

- Ligne 28 : `<form method="post" action="/settings/team/leave" onsubmit="return confirm('Quitter le compte de {{ appartenance.owner_email }} ?');">`
- Ligne 63 : `<form method="post" action="/settings/team/member/{{ m.user_id }}/remove" onsubmit="return confirm('Retirer {{ m.email }} de ton compte ?');">`
- Ligne 91 : `<form method="post" action="/settings/team/invite/{{ i.id }}/revoke">`
- Ligne 108 : `<form method="post" action="/settings/team/invite" class="row">`

### status_public.html

Template de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### support_public.html

Template de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### terms_public.html

Template de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

## Variantes des articles publics

Une ligne par contenu versionné. La preuve navigateur de chaque variante est indiquée dans l’audit, pas déduite de sa présence dans ce tableau.

| URL | Contenu source |
| --- | --- |
| `/docs/ajouter-un-projet` | [ajouter-un-projet.md](../content/docs/ajouter-un-projet.md) |
| `/docs/analyser-les-concurrents` | [analyser-les-concurrents.md](../content/docs/analyser-les-concurrents.md) |
| `/docs/anomalies-et-priorites` | [anomalies-et-priorites.md](../content/docs/anomalies-et-priorites.md) |
| `/docs/automatiser-les-audits` | [automatiser-les-audits.md](../content/docs/automatiser-les-audits.md) |
| `/docs/backlinks` | [backlinks.md](../content/docs/backlinks.md) |
| `/docs/comptes-d-equipe` | [comptes-d-equipe.md](../content/docs/comptes-d-equipe.md) |
| `/docs/connecter-github` | [connecter-github.md](../content/docs/connecter-github.md) |
| `/docs/connecter-search-console` | [connecter-search-console.md](../content/docs/connecter-search-console.md) |
| `/docs/corrections-automatiques` | [corrections-automatiques.md](../content/docs/corrections-automatiques.md) |
| `/docs/demarrer` | [demarrer.md](../content/docs/demarrer.md) |
| `/docs/depannage` | [depannage.md](../content/docs/depannage.md) |
| `/docs/exports-et-rapports` | [exports-et-rapports.md](../content/docs/exports-et-rapports.md) |
| `/docs/fix-pack-sans-depot` | [fix-pack-sans-depot.md](../content/docs/fix-pack-sans-depot.md) |
| `/docs/lancer-un-crawl` | [lancer-un-crawl.md](../content/docs/lancer-un-crawl.md) |
| `/docs/lire-le-rapport` | [lire-le-rapport.md](../content/docs/lire-le-rapport.md) |
| `/docs/opportunites-de-mots-cles` | [opportunites-de-mots-cles.md](../content/docs/opportunites-de-mots-cles.md) |
| `/docs/plans-et-quotas` | [plans-et-quotas.md](../content/docs/plans-et-quotas.md) |
| `/docs/rediger-du-contenu` | [rediger-du-contenu.md](../content/docs/rediger-du-contenu.md) |
| `/docs/suivre-la-performance` | [suivre-la-performance.md](../content/docs/suivre-la-performance.md) |
| `/docs/verifier-une-correction` | [verifier-une-correction.md](../content/docs/verifier-une-correction.md) |
| `/ressources-seo/audit-seo-technique-checklist-priorites` | [audit-seo-technique-checklist-priorites.md](../content/blog/audit-seo-technique-checklist-priorites.md) |
| `/ressources-seo/ce-que-vos-concurrents-publient-et-pas-vous` | [ce-que-vos-concurrents-publient-et-pas-vous.md](../content/blog/ce-que-vos-concurrents-publient-et-pas-vous.md) |
| `/ressources-seo/connecter-google-search-console-audit-mensuel` | [connecter-google-search-console-audit-mensuel.md](../content/blog/connecter-google-search-console-audit-mensuel.md) |
| `/ressources-seo/core-web-vitals-lire-signaux-seo` | [core-web-vitals-lire-signaux-seo.md](../content/blog/core-web-vitals-lire-signaux-seo.md) |
| `/ressources-seo/corriger-le-seo-dans-le-code-pas-dans-un-rapport` | [corriger-le-seo-dans-le-code-pas-dans-un-rapport.md](../content/blog/corriger-le-seo-dans-le-code-pas-dans-un-rapport.md) |
| `/ressources-seo/corriger-title-meta-description-grande-echelle` | [corriger-title-meta-description-grande-echelle.md](../content/blog/corriger-title-meta-description-grande-echelle.md) |
| `/ressources-seo/frequence-crawl-seo-site-vitrine-ecommerce-blog` | [frequence-crawl-seo-site-vitrine-ecommerce-blog.md](../content/blog/frequence-crawl-seo-site-vitrine-ecommerce-blog.md) |
| `/ressources-seo/netlinking-opportunites-backlinks-sans-spam` | [netlinking-opportunites-backlinks-sans-spam.md](../content/blog/netlinking-opportunites-backlinks-sans-spam.md) |
| `/ressources-seo/regression-seo-apres-mise-en-production` | [regression-seo-apres-mise-en-production.md](../content/blog/regression-seo-apres-mise-en-production.md) |
