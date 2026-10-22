# Inventaire courant des écrans et interactions Noyaru

Généré par `python seo-agent-web/ops/ux_inventory.py`. Révision du code : `6fd9b2effc3084f7b8c8eba2356e804e82ef772e`. Date UTC : 2026-10-22 03:42. Le [relevé initial et ses compléments](UX_SCREEN_INVENTORY.md) sont conservés.

198 déclarations de routes ; 55 templates repérés dans les gestionnaires de routes ; 89 templates HTML au total (inclusions et anciens templates compris). 3 routes automatiques FastAPI complètent les déclarations, soit 201 déclarations et routes automatiques, hors montage statique. Les alias de langue du middleware ne sont pas des déclarations supplémentaires.

Les verbes sont ceux déclarés dans le code. Une route peut servir plusieurs états ou rediriger. Les droits sont déterminés par les middlewares et les contrôles de projet/compte, pas par cette extraction. Une référence littérale ou une variante du résolveur juridique n’atteste pas son exécution. Les rendus entièrement dynamiques peuvent ne pas être attribués à une route par cette extraction. Voir [le bilan transversal](UX_CROSS_JOURNEY_LOT16.md) pour les vérifications et leurs limites.

## Routes, écrans, actions et sorties

Les API, exports, callbacks et actions POST sont inclus pour rendre visibles les étapes intermédiaires. Le montage des fichiers statiques est distingué des écrans. Les routes automatiques du framework sont recensées séparément ci-dessous.

| Route | Méthode | Template(s) | Source et fonction |
| --- | --- | --- | --- |
| `/` | GET, HEAD | home_public.html, projects.html | [backend/app.py:14875](../backend/app.py#L14875) · `projects` |
| `/api/assistant/chat` | POST | — | [backend/app.py:16529](../backend/app.py#L16529) · `assistant_chat` |
| `/api/assistant/meta` | GET | — | [backend/app.py:16518](../backend/app.py#L16518) · `assistant_meta` |
| `/api/automation/domains` | GET | — | [backend/app.py:18088](../backend/app.py#L18088) · `api_automation_domains_get` |
| `/api/automation/domains` | POST | — | [backend/app.py:18102](../backend/app.py#L18102) · `api_automation_domains_save` |
| `/api/automation/github-corrections` | GET | — | [backend/app.py:18131](../backend/app.py#L18131) · `api_automation_github_corrections` |
| `/api/bing/sites` | GET | — | [backend/app.py:17833](../backend/app.py#L17833) · `bing_sites` |
| `/api/github/repos` | GET | — | [backend/app.py:17000](../backend/app.py#L17000) · `github_repos` |
| `/api/gsc/properties` | GET | — | [backend/app.py:17603](../backend/app.py#L17603) · `gsc_properties` |
| `/api/gsc/properties/via-oauth` | GET | — | [backend/app.py:17713](../backend/app.py#L17713) · `gsc_properties_via_oauth` |
| `/api/jobs/{job_id}` | GET | — | [backend/app.py:37629](../backend/app.py#L37629) · `job_api` |
| `/api/netlify/sites` | GET | — | [backend/app.py:17172](../backend/app.py#L17172) · `netlify_sites` |
| `/api/projects/{slug}/backlinks/auto-settings` | POST | — | [backend/app.py:37054](../backend/app.py#L37054) · `backlinks_auto_settings_save` |
| `/api/projects/{slug}/backlinks/generate-reply` | POST | — | [backend/app.py:36844](../backend/app.py#L36844) · `api_backlinks_generate_reply` |
| `/api/projects/{slug}/backlinks/queue/{opp_id}/approve` | POST | — | [backend/app.py:37099](../backend/app.py#L37099) · `backlinks_queue_approve` |
| `/api/projects/{slug}/backlinks/queue/{opp_id}/mark-sent` | POST | — | [backend/app.py:37135](../backend/app.py#L37135) · `backlinks_queue_mark_sent` |
| `/api/projects/{slug}/backlinks/queue/{opp_id}/reject` | POST | — | [backend/app.py:37117](../backend/app.py#L37117) · `backlinks_queue_reject` |
| `/api/projects/{slug}/backlinks/search` | POST | — | [backend/app.py:36775](../backend/app.py#L36775) · `api_backlinks_search` |
| `/api/projects/{slug}/bing/sites` | GET | — | [backend/app.py:17513](../backend/app.py#L17513) · `bing_sites_for_project` |
| `/api/projects/{slug}/competitors/etat` | GET | — | [backend/app.py:35213](../backend/app.py#L35213) · `api_competitors_etat` |
| `/api/projects/{slug}/competitors/suggest` | POST | — | [backend/app.py:35155](../backend/app.py#L35155) · `api_competitors_suggest` |
| `/api/projects/{slug}/content/article` | POST | — | [backend/app.py:34494](../backend/app.py#L34494) · `api_content_article` |
| `/api/projects/{slug}/content/article/{task_id}` | GET | — | [backend/app.py:34666](../backend/app.py#L34666) · `api_content_article_lire` |
| `/api/projects/{slug}/content/auto` | POST | — | [backend/app.py:35729](../backend/app.py#L35729) · `api_content_auto_settings` |
| `/api/projects/{slug}/content/draft` | POST | — | [backend/app.py:33980](../backend/app.py#L33980) · `api_content_draft` |
| `/api/projects/{slug}/content/langues` | GET | — | [backend/app.py:34053](../backend/app.py#L34053) · `api_content_langues` |
| `/api/projects/{slug}/content/suivi/{jeton}` | GET | — | [backend/app.py:33278](../backend/app.py#L33278) · `api_content_suivi` |
| `/api/projects/{slug}/content/sujet` | POST | — | [backend/app.py:34625](../backend/app.py#L34625) · `api_content_sujet` |
| `/api/projects/{slug}/github/bulk-fix` | POST | — | [backend/app.py:19833](../backend/app.py#L19833) · `api_github_bulk_fix` |
| `/api/projects/{slug}/github/connect` | POST | — | [backend/app.py:19563](../backend/app.py#L19563) · `api_github_connect` |
| `/api/projects/{slug}/github/status` | GET | — | [backend/app.py:19594](../backend/app.py#L19594) · `api_github_status` |
| `/api/projects/{slug}/gsc/properties` | GET | — | [backend/app.py:17393](../backend/app.py#L17393) · `gsc_properties_for_project` |
| `/api/projects/{slug}/issues/{issue_key}/deep-fix` | POST | — | [backend/app.py:31168](../backend/app.py#L31168) · `api_issue_deep_fix` |
| `/api/projects/{slug}/issues/{issue_key}/github-fix` | POST | — | [backend/app.py:19602](../backend/app.py#L19602) · `api_github_fix` |
| `/api/projects/{slug}/issues/{issue_key}/task` | POST | — | [backend/app.py:31414](../backend/app.py#L31414) · `api_issue_task_upsert` |
| `/api/projects/{slug}/issues/{issue_key}/url-fix` | GET | — | [backend/app.py:19437](../backend/app.py#L19437) · `api_issue_url_fix` |
| `/api/projects/{slug}/keywords/rewrite-pr` | POST | — | [backend/app.py:33048](../backend/app.py#L33048) · `api_keyword_rewrite_pr` |
| `/api/projects/{slug}/search-items` | GET | — | [backend/app.py:17957](../backend/app.py#L17957) · `project_search_items` |
| `/api/projects/{slug}/search-series` | GET | — | [backend/app.py:17898](../backend/app.py#L17898) · `project_search_series` |
| `/api/projects/{slug}/tasks` | GET | — | [backend/app.py:31448](../backend/app.py#L31448) · `api_project_tasks` |
| `/api/settings/activation` | GET | — | [backend/activation_routes.py:181](../backend/activation_routes.py#L181) · `activation_api` |
| `/api/settings/activation/plans` | GET | — | [backend/activation_routes.py:73](../backend/activation_routes.py#L73) · `current_plan_counts` |
| `/api/settings/operations` | GET | — | [backend/app.py:16021](../backend/app.py#L16021) · `api_settings_operations` |
| `/apple-touch-icon.png` | GET, HEAD | — | [backend/app.py:10431](../backend/app.py#L10431) · `apple_touch_icon` |
| `/auth/forgot` | GET | auth_forgot.html | [backend/app.py:12554](../backend/app.py#L12554) · `auth_forgot` |
| `/auth/forgot` | POST | auth_forgot.html | [backend/app.py:12585](../backend/app.py#L12585) · `auth_forgot_submit` |
| `/auth/google/callback` | GET | — | [backend/app.py:13140](../backend/app.py#L13140) · `auth_google_callback` |
| `/auth/google/start` | POST | — | [backend/app.py:13053](../backend/app.py#L13053) · `auth_google_start` |
| `/auth/login` | GET | auth_login.html | [backend/app.py:13451](../backend/app.py#L13451) · `auth_login` |
| `/auth/login` | POST | auth_login.html | [backend/app.py:13481](../backend/app.py#L13481) · `auth_login_submit` |
| `/auth/logout` | POST | — | [backend/app.py:13858](../backend/app.py#L13858) · `auth_logout` |
| `/auth/reset` | GET | auth_reset.html | [backend/app.py:12703](../backend/app.py#L12703) · `auth_reset` |
| `/auth/reset` | POST | auth_reset.html | [backend/app.py:12734](../backend/app.py#L12734) · `auth_reset_submit` |
| `/auth/signup` | GET | auth_signup.html | [backend/app.py:13609](../backend/app.py#L13609) · `auth_signup` |
| `/auth/signup` | POST | — | [backend/app.py:13656](../backend/app.py#L13656) · `auth_signup_submit` |
| `/auth/verify` | GET | auth_verify.html | [backend/app.py:12953](../backend/app.py#L12953) · `auth_verify` |
| `/auth/verify/resend` | GET | auth_verify_resend.html | [backend/app.py:12826](../backend/app.py#L12826) · `auth_verify_resend` |
| `/auth/verify/resend` | POST | auth_verify_resend.html | [backend/app.py:12857](../backend/app.py#L12857) · `auth_verify_resend_submit` |
| `/automation` | GET | automation.html | [backend/app.py:18029](../backend/app.py#L18029) · `automation` |
| `/billing` | GET | billing.html | [backend/app.py:14430](../backend/app.py#L14430) · `billing_page` |
| `/billing/cancel-scheduled-change` | POST | — | [backend/app.py:14712](../backend/app.py#L14712) · `billing_cancel_scheduled_change` |
| `/billing/checkout` | POST | — | [backend/app.py:14601](../backend/app.py#L14601) · `billing_checkout` |
| `/billing/portal` | POST | — | [backend/app.py:14752](../backend/app.py#L14752) · `billing_portal` |
| `/cron/auto-content` | POST | — | [backend/app.py:35915](../backend/app.py#L35915) · `cron_auto_content` |
| `/cron/auto-post-backlinks` | GET | — | [backend/app.py:37445](../backend/app.py#L37445) · `cron_auto_post_backlinks` |
| `/cron/auto-search-backlinks` | GET | — | [backend/app.py:37268](../backend/app.py#L37268) · `cron_auto_search_backlinks` |
| `/cron/autopilot` | GET | — | [backend/app.py:18170](../backend/app.py#L18170) · `cron_autopilot` |
| `/cron/check-backlinks` | GET | — | [backend/app.py:37178](../backend/app.py#L37178) · `cron_check_backlinks` |
| `/cron/refresh-competitors` | POST | — | [backend/app.py:35341](../backend/app.py#L35341) · `cron_refresh_competitors` |
| `/cron/verify-pull-requests` | POST | — | [backend/app.py:38387](../backend/app.py#L38387) · `cron_verify_pull_requests` |
| `/docs` | GET, HEAD | docs_index.html | [backend/app.py:14265](../backend/app.py#L14265) · `docs_index_public` |
| `/docs/{slug}` | GET, HEAD | docs_article.html | [backend/app.py:14291](../backend/app.py#L14291) · `docs_article_public` |
| `/favicon.ico` | GET, HEAD | — | [backend/app.py:10426](../backend/app.py#L10426) · `favicon_ico` |
| `/file` | GET | file.html | [backend/app.py:18374](../backend/app.py#L18374) · `view_file` |
| `/healthz` | GET | — | [backend/app.py:11901](../backend/app.py#L11901) · `healthz` |
| `/jobs` | GET | jobs.html | [backend/app.py:18262](../backend/app.py#L18262) · `jobs` |
| `/jobs/{job_id}` | GET | job.html | [backend/app.py:37453](../backend/app.py#L37453) · `job_detail` |
| `/jobs/{job_id}/apply-corrections` | POST | — | [backend/app.py:37850](../backend/app.py#L37850) · `apply_corrections` |
| `/jobs/{job_id}/cancel` | POST | — | [backend/app.py:37554](../backend/app.py#L37554) · `job_cancel` |
| `/jobs/{job_id}/retry` | POST | — | [backend/app.py:37581](../backend/app.py#L37581) · `job_retry` |
| `/mentions-legales` | GET, HEAD | legal_notice_public.html, legal_notice_public_en.html | [backend/app.py:14142](../backend/app.py#L14142) · `legal_notice_public` |
| `/oauth/bing/callback` | GET | — | [backend/app.py:17297](../backend/app.py#L17297) · `bing_oauth_callback` |
| `/oauth/bing/connect` | GET | — | [backend/app.py:17257](../backend/app.py#L17257) · `bing_oauth_connect` |
| `/oauth/bing/disconnect` | POST | — | [backend/app.py:17376](../backend/app.py#L17376) · `bing_oauth_disconnect` |
| `/oauth/github/callback` | GET | — | [backend/app.py:16919](../backend/app.py#L16919) · `github_oauth_callback` |
| `/oauth/github/connect` | GET | — | [backend/app.py:16885](../backend/app.py#L16885) · `github_oauth_connect` |
| `/oauth/github/disconnect` | POST | — | [backend/app.py:16990](../backend/app.py#L16990) · `github_oauth_disconnect` |
| `/oauth/google/callback` | GET | — | [backend/app.py:16676](../backend/app.py#L16676) · `google_oauth_callback` |
| `/oauth/netlify/callback` | GET | oauth_fragment_callback.html | [backend/app.py:17086](../backend/app.py#L17086) · `netlify_oauth_callback_page` |
| `/oauth/netlify/callback/complete` | POST | — | [backend/app.py:17103](../backend/app.py#L17103) · `netlify_oauth_complete` |
| `/oauth/netlify/connect` | GET | — | [backend/app.py:17055](../backend/app.py#L17055) · `netlify_oauth_connect` |
| `/oauth/netlify/disconnect` | POST | — | [backend/app.py:17162](../backend/app.py#L17162) · `netlify_oauth_disconnect` |
| `/pricing` | GET, HEAD | pricing_public.html | [backend/app.py:14060](../backend/app.py#L14060) · `pricing_public` |
| `/privacy` | GET, HEAD | privacy_public.html, privacy_public_en.html | [backend/app.py:14125](../backend/app.py#L14125) · `privacy_public` |
| `/projects/add` | GET | — | [backend/app.py:16318](../backend/app.py#L16318) · `add_project_entry` |
| `/projects/add` | POST | — | [backend/app.py:16327](../backend/app.py#L16327) · `add_project` |
| `/projects/crawl-batch` | POST | — | [backend/app.py:18641](../backend/app.py#L18641) · `crawl_projects_batch` |
| `/projects/delete` | POST | — | [backend/app.py:16457](../backend/app.py#L16457) · `delete_projects` |
| `/projects/first-audit` | POST | — | [backend/first_audit.py:138](../backend/first_audit.py#L138) · `start` |
| `/projects/{slug}` | GET | project_overview.html | [backend/app.py:18903](../backend/app.py#L18903) · `project_overview` |
| `/projects/{slug}/automation` | GET | project_automation.html | [backend/app.py:31470](../backend/app.py#L31470) · `project_automation` |
| `/projects/{slug}/backlinks` | GET | backlinks.html | [backend/app.py:36062](../backend/app.py#L36062) · `project_backlinks` |
| `/projects/{slug}/backlinks/ahrefs/sync` | POST | — | [backend/app.py:36426](../backend/app.py#L36426) · `backlinks_ahrefs_sync` |
| `/projects/{slug}/backlinks/clear` | POST | — | [backend/app.py:36388](../backend/app.py#L36388) · `backlinks_clear` |
| `/projects/{slug}/backlinks/import` | POST | — | [backend/app.py:36297](../backend/app.py#L36297) · `backlinks_import` |
| `/projects/{slug}/backlinks/opportunities` | GET | backlinks_opportunities.html | [backend/app.py:36677](../backend/app.py#L36677) · `project_backlinks_opportunities` |
| `/projects/{slug}/backlinks/opportunities/save` | POST | — | [backend/app.py:36913](../backend/app.py#L36913) · `backlinks_opportunity_save` |
| `/projects/{slug}/backlinks/opportunities/{opp_id}/delete` | POST | — | [backend/app.py:36982](../backend/app.py#L36982) · `backlinks_opportunity_delete` |
| `/projects/{slug}/backlinks/opportunities/{opp_id}/status` | POST | — | [backend/app.py:37005](../backend/app.py#L37005) · `backlinks_opportunity_status` |
| `/projects/{slug}/cms` | GET | cms_settings.html | [backend/cms_routes.py:133](../backend/cms_routes.py#L133) · `cms_settings` |
| `/projects/{slug}/cms/platform` | POST | — | [backend/cms_routes.py:149](../backend/cms_routes.py#L149) · `cms_platform` |
| `/projects/{slug}/cms/wordpress/changes/{change_id}` | GET | cms_wordpress_change.html | [backend/cms_routes.py:339](../backend/cms_routes.py#L339) · `wordpress_change` |
| `/projects/{slug}/cms/wordpress/changes/{change_id}/status` | GET | — | [backend/cms_routes.py:445](../backend/cms_routes.py#L445) · `wordpress_change_status` |
| `/projects/{slug}/cms/wordpress/changes/{change_id}/{action}` | POST | — | [backend/cms_routes.py:549](../backend/cms_routes.py#L549) · `wordpress_change_action` |
| `/projects/{slug}/cms/wordpress/connect` | POST | — | [backend/cms_routes.py:170](../backend/cms_routes.py#L170) · `wordpress_connect` |
| `/projects/{slug}/cms/wordpress/disconnect` | POST | — | [backend/cms_routes.py:223](../backend/cms_routes.py#L223) · `wordpress_disconnect` |
| `/projects/{slug}/cms/wordpress/edit` | GET | cms_wordpress_edit.html | [backend/cms_routes.py:236](../backend/cms_routes.py#L236) · `wordpress_edit` |
| `/projects/{slug}/cms/wordpress/issues/{issue_key}` | GET | cms_wordpress_batch.html | [backend/cms_routes.py:691](../backend/cms_routes.py#L691) · `wordpress_batch` |
| `/projects/{slug}/cms/wordpress/issues/{issue_key}/apply` | POST | — | [backend/cms_routes.py:739](../backend/cms_routes.py#L739) · `wordpress_batch_apply` |
| `/projects/{slug}/cms/wordpress/issues/{issue_key}/propose` | POST | — | [backend/cms_routes.py:718](../backend/cms_routes.py#L718) · `wordpress_batch_propose` |
| `/projects/{slug}/cms/wordpress/issues/{issue_key}/verify` | POST | — | [backend/cms_routes.py:792](../backend/cms_routes.py#L792) · `wordpress_batch_verify` |
| `/projects/{slug}/cms/wordpress/preview` | POST | — | [backend/cms_routes.py:261](../backend/cms_routes.py#L261) · `wordpress_preview` |
| `/projects/{slug}/cms/wordpress/suggest` | POST | — | [backend/cms_routes.py:310](../backend/cms_routes.py#L310) · `wordpress_suggest` |
| `/projects/{slug}/competitors` | GET | competitors.html | [backend/app.py:34987](../backend/app.py#L34987) · `project_competitors` |
| `/projects/{slug}/competitors/add` | POST | — | [backend/app.py:35238](../backend/app.py#L35238) · `project_competitor_add` |
| `/projects/{slug}/competitors/analyze` | POST | — | [backend/app.py:35297](../backend/app.py#L35297) · `project_competitor_analyze` |
| `/projects/{slug}/competitors/remove` | POST | — | [backend/app.py:35279](../backend/app.py#L35279) · `project_competitor_remove` |
| `/projects/{slug}/content` | GET | content.html | [backend/app.py:34814](../backend/app.py#L34814) · `project_content` |
| `/projects/{slug}/content/valider/{task_id}` | GET | content_valider.html | [backend/app.py:35831](../backend/app.py#L35831) · `page_contenu_valider` |
| `/projects/{slug}/content/valider/{task_id}` | POST | — | [backend/app.py:35855](../backend/app.py#L35855) · `page_contenu_valider_post` |
| `/projects/{slug}/corrections` | GET | corrections.html | [backend/app.py:31668](../backend/app.py#L31668) · `project_corrections` |
| `/projects/{slug}/crawl` | POST | — | [backend/app.py:18502](../backend/app.py#L18502) · `crawl_project` |
| `/projects/{slug}/crawls` | GET | crawls.html | [backend/app.py:32379](../backend/app.py#L32379) · `project_crawls` |
| `/projects/{slug}/export/fix-pack.zip` | GET | — | [backend/app.py:31915](../backend/app.py#L31915) · `export_project_fix_pack_zip` |
| `/projects/{slug}/export/issues-all-urls.csv` | GET | — | [backend/app.py:32028](../backend/app.py#L32028) · `export_project_issues_all_urls_csv` |
| `/projects/{slug}/export/issues.csv` | GET | — | [backend/app.py:31959](../backend/app.py#L31959) · `export_project_issues_csv` |
| `/projects/{slug}/export/issues.pdf` | GET | — | [backend/app.py:32151](../backend/app.py#L32151) · `export_project_issues_pdf` |
| `/projects/{slug}/export/issues/{issue_key}.csv` | GET | — | [backend/app.py:32216](../backend/app.py#L32216) · `export_project_issue_csv` |
| `/projects/{slug}/export/issues/{issue_key}.pdf` | GET | — | [backend/app.py:32309](../backend/app.py#L32309) · `export_project_issue_pdf` |
| `/projects/{slug}/export/report.csv` | GET | — | [backend/app.py:31833](../backend/app.py#L31833) · `export_project_report_csv` |
| `/projects/{slug}/export/report.pdf` | GET | — | [backend/app.py:31872](../backend/app.py#L31872) · `export_project_report_pdf` |
| `/projects/{slug}/first-correction` | GET | first_correction.html | [backend/app.py:31579](../backend/app.py#L31579) · `project_first_correction` |
| `/projects/{slug}/first-correction` | POST | — | [backend/app.py:31592](../backend/app.py#L31592) · `project_first_correction_update` |
| `/projects/{slug}/fix-suggestions/generate` | POST | — | [backend/app.py:19307](../backend/app.py#L19307) · `project_generate_fix_suggestions` |
| `/projects/{slug}/gsc/oauth/connect` | GET | — | [backend/app.py:16637](../backend/app.py#L16637) · `project_gsc_oauth_connect` |
| `/projects/{slug}/gsc/oauth/disconnect` | POST | — | [backend/app.py:16838](../backend/app.py#L16838) · `project_gsc_oauth_disconnect` |
| `/projects/{slug}/issues` | GET | issues.html | [backend/app.py:19248](../backend/app.py#L19248) · `project_issues` |
| `/projects/{slug}/issues/{issue_key}` | GET | issue_detail.html | [backend/app.py:19342](../backend/app.py#L19342) · `project_issue_detail` |
| `/projects/{slug}/keywords` | GET | performance.html | [backend/app.py:35935](../backend/app.py#L35935) · `project_performance` |
| `/projects/{slug}/keywords/opportunities` | GET | keyword_opportunities.html | [backend/app.py:32884](../backend/app.py#L32884) · `project_keyword_opportunities` |
| `/projects/{slug}/keywords/track` | POST | — | [backend/app.py:32962](../backend/app.py#L32962) · `project_keyword_track` |
| `/projects/{slug}/keywords/untrack` | POST | — | [backend/app.py:33008](../backend/app.py#L33008) · `project_keyword_untrack` |
| `/projects/{slug}/netlinking` | GET | backlinks.html | [backend/app.py:36062](../backend/app.py#L36062) · `project_backlinks` |
| `/projects/{slug}/performance` | GET | performance.html | [backend/app.py:35935](../backend/app.py#L35935) · `project_performance` |
| `/projects/{slug}/search-connections/{source}` | POST | — | [backend/app.py:18869](../backend/app.py#L18869) · `project_search_connection_update` |
| `/projects/{slug}/settings/crawl` | GET | crawl_settings.html | [backend/app.py:19038](../backend/app.py#L19038) · `project_crawl_settings` |
| `/projects/{slug}/settings/crawl` | POST | — | [backend/app.py:19124](../backend/app.py#L19124) · `project_crawl_settings_save` |
| `/projects/{slug}/settings/search` | GET | settings_search.html | [backend/app.py:18856](../backend/app.py#L18856) · `project_search_settings` |
| `/ressources-seo` | GET, HEAD | seo_resources_public.html | [backend/app.py:14322](../backend/app.py#L14322) · `seo_resources_public` |
| `/ressources-seo/{slug}` | GET, HEAD | seo_resource_article_public.html | [backend/app.py:14340](../backend/app.py#L14340) · `seo_resource_article_public` |
| `/robots.txt` | GET, HEAD | — | [backend/app.py:14367](../backend/app.py#L14367) · `robots_txt` |
| `/run` | POST | — | [backend/app.py:18423](../backend/app.py#L18423) · `run` |
| `/settings` | GET | — | [backend/app.py:15036](../backend/app.py#L15036) · `settings_root` |
| `/settings/account/attribution/withdraw` | POST | — | [backend/activation_routes.py:234](../backend/activation_routes.py#L234) · `withdraw_attribution` |
| `/settings/account/country` | POST | — | [backend/activation_routes.py:114](../backend/activation_routes.py#L114) · `save_own_country` |
| `/settings/account/delete` | POST | — | [backend/app.py:15226](../backend/app.py#L15226) · `settings_account_delete` |
| `/settings/account/password` | POST | — | [backend/app.py:15365](../backend/app.py#L15365) · `settings_account_password` |
| `/settings/account/preferences` | POST | — | [backend/app.py:15432](../backend/app.py#L15432) · `settings_account_preferences_save` |
| `/settings/accounts` | GET | settings_accounts.html | [backend/app.py:15066](../backend/app.py#L15066) · `settings_accounts` |
| `/settings/accounts` | POST | — | [backend/app.py:15182](../backend/app.py#L15182) · `settings_accounts_save` |
| `/settings/accounts/gsc/disconnect-all` | POST | — | [backend/app.py:16862](../backend/app.py#L16862) · `gsc_disconnect_all_projects` |
| `/settings/activation` | GET | settings_activation.html | [backend/activation_routes.py:48](../backend/activation_routes.py#L48) · `activation_page` |
| `/settings/activation.csv` | GET | — | [backend/activation_routes.py:188](../backend/activation_routes.py#L188) · `activation_csv` |
| `/settings/activation/accounts` | GET | settings_customer_accounts.html | [backend/activation_routes.py:99](../backend/activation_routes.py#L99) · `accounts_page` |
| `/settings/activation/accounts.csv` | GET | — | [backend/activation_routes.py:132](../backend/activation_routes.py#L132) · `accounts_csv` |
| `/settings/activation/accounts/{account_id}` | GET | settings_customer_account_profile.html | [backend/activation_routes.py:163](../backend/activation_routes.py#L163) · `account_profile_page` |
| `/settings/activation/accounts/{account_id}/export.csv` | GET | — | [backend/activation_routes.py:154](../backend/activation_routes.py#L154) · `account_profile_csv` |
| `/settings/activation/acquisition.csv` | GET | — | [backend/activation_routes.py:204](../backend/activation_routes.py#L204) · `acquisition_csv` |
| `/settings/activation/campaigns` | POST | — | [backend/activation_routes.py:221](../backend/activation_routes.py#L221) · `create_campaign` |
| `/settings/legal-notices` | GET | settings_legal_notices.html | [backend/legal_notice_routes.py:31](../backend/legal_notice_routes.py#L31) · `legal_notices_page` |
| `/settings/legal-notices` | POST | — | [backend/legal_notice_routes.py:59](../backend/legal_notice_routes.py#L59) · `announce` |
| `/settings/legal-notices/{notice_id}/resume` | POST | — | [backend/legal_notice_routes.py:81](../backend/legal_notice_routes.py#L81) · `resume` |
| `/settings/operations` | GET | settings_operations.html | [backend/app.py:16002](../backend/app.py#L16002) · `settings_operations` |
| `/settings/system` | GET | settings_system.html | [backend/app.py:16027](../backend/app.py#L16027) · `settings_system` |
| `/settings/system` | POST | — | [backend/app.py:16259](../backend/app.py#L16259) · `settings_system_save` |
| `/settings/system/secrets/rotate` | POST | — | [backend/app.py:16219](../backend/app.py#L16219) · `settings_system_rotate_secrets` |
| `/settings/team` | GET | settings_team.html | [backend/app.py:37962](../backend/app.py#L37962) · `settings_team` |
| `/settings/team/invite` | POST | — | [backend/app.py:38029](../backend/app.py#L38029) · `settings_team_invite` |
| `/settings/team/invite/{invite_id}/revoke` | POST | — | [backend/app.py:38069](../backend/app.py#L38069) · `settings_team_invite_revoke` |
| `/settings/team/leave` | POST | — | [backend/app.py:38095](../backend/app.py#L38095) · `settings_team_leave` |
| `/settings/team/member/{member_id}/remove` | POST | — | [backend/app.py:38083](../backend/app.py#L38083) · `settings_team_member_remove` |
| `/site.webmanifest` | GET, HEAD | — | [backend/app.py:10436](../backend/app.py#L10436) · `site_webmanifest` |
| `/sitemap.xml` | GET, HEAD | — | [backend/app.py:14394](../backend/app.py#L14394) · `sitemap_xml` |
| `/status` | GET, HEAD | status_public.html | [backend/app.py:14166](../backend/app.py#L14166) · `status_public` |
| `/stripe/webhook` | POST | — | [backend/app.py:14828](../backend/app.py#L14828) · `stripe_webhook` |
| `/support` | GET, HEAD | support_public.html | [backend/app.py:14151](../backend/app.py#L14151) · `support_public` |
| `/team/accept` | GET | — | [backend/app.py:38120](../backend/app.py#L38120) · `team_accept` |
| `/terms` | GET, HEAD | terms_public.html, terms_public_en.html | [backend/app.py:14099](../backend/app.py#L14099) · `terms_public` |

## Routes automatiques de FastAPI

Ces chemins sont extraits de la configuration FastAPI. Le rapprochement avec le registre effectivement chargé est consigné dans les preuves du bilan transversal, pas déduit de ce relevé. Ils ne passent pas par un template du dépôt. L’explorateur et son schéma exigent une session via le middleware ; le callback Swagger reste sous le préfixe public `/docs`. Aucun accès administrateur supplémentaire n’est déclaré pour l’explorateur. Le chemin de retour OAuth2 ci-dessous est le défaut de la version FastAPI utilisée par cette application.

| Route | Méthodes | Configuration source |
| --- | --- | --- |
| `/internal/api-explorer` | GET, HEAD | `docs_url` · [backend/app.py:10394](../backend/app.py#L10394) |
| `/internal/openapi.json` | GET, HEAD | `openapi_url` · [backend/app.py:10394](../backend/app.py#L10394) |
| `/docs/oauth2-redirect` | GET, HEAD | `swagger_ui_oauth2_redirect_url (défaut FastAPI)` · [backend/app.py:10394](../backend/app.py#L10394) |

## Fenêtres, formulaires et panneaux par template

Extraction statique des balises `form`, `dialog`, `details` et des régions `role=dialog`. Les expressions Jinja sont conservées. Les boutons pilotés par JavaScript sont examinés dans le bilan ; cette liste n’atteste ni l’ouverture d’une fenêtre ni la soumission d’un formulaire.

### _admin_scope.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### _auth_entry_intro.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### _auth_recovery_footer.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### _capture.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### _cms_guidance.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### _cms_workflow.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### _comparatif.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### _content_styles.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### _correction_progress.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### _correction_review.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

- Ligne 4 : `<dialog class="correction-review" id="correction-review" aria-labelledby="review-title" aria-describedby="review-intro">`

### _country_field.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### _customer_follow_up.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

- Ligne 3 : `<form method="get" action="/settings/activation#clients-a-accompagner">`
- Ligne 9 : `<details class="follow-up-method">`

### _favicons.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### _first_audit_start.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

- Ligne 6 : `<form method="post" action="/projects/first-audit" data-audit-submit="{{ tr('Démarrage de l’analyse…') }}">`
- Ligne 24 : `<details class="first-audit-options">`

### _lang_script.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### _lang_switch.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### _legal_styles.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### _next_step.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

- Ligne 10 : `<form method="post" action="{{ next_step.href }}" data-audit-submit="{{ tr('Démarrage de l’analyse…') }}">`
- Ligne 30 : `<details class="journey-help">`

### _overview_history.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

- Ligne 44 : `<details class="overview-history-records">`

### _plan_usage.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

- Ligne 17 : `<details>`
- Ligne 18 : `<details>`
- Ligne 19 : `<details>`

### _repli_de_rapport.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### _report_exports.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

- Ligne 9 : `<details class="report-exports">`
- Ligne 41 : `<details class="report-exports-technical">`

### _search_connection_badge.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

- Ligne 4 : `<form method="post" action="/projects/{{ slug }}/search-connections/{{ connection.source }}">`
- Ligne 18 : `<form method="post" action="/projects/{{ slug }}/search-connections/{{ connection.source }}">`
- Ligne 30 : `<details class="search-connection" id="{{ connection.source }}-connection">`

### _search_metrics_help.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

- Ligne 1 : `<details class="screen-options metric-explanations">`

### _search_unavailable.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

- Ligne 8 : `<details class="technical-details">`

### _site_add_form.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

- Ligne 1 : `<form method="post" action="/projects/add" class="site-add-form">`

### _tarif_fonctions.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### _tarifs.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

- Ligne 40 : `<details class="tarifs-detail">`

### _tarifs_styles.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### auth_forgot.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 21 : `<details class="auth-entry-optional auth-recovery-resend">`
- Ligne 23 : `<form method="post" action="/auth/forgot" class="auth-entry-form">`

### auth_login.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 19 : `<form method="post" action="/auth/login" class="auth-entry-form">`

### auth_reset.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 19 : `<form method="post" action="/auth/reset" class="auth-entry-form">`

### auth_signup.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 25 : `<form method="post" action="/auth/signup" class="auth-entry-form">`
- Ligne 51 : `<details class="auth-entry-optional">`

### auth_verify.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### auth_verify_resend.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 21 : `<details class="auth-entry-optional auth-recovery-resend">`
- Ligne 23 : `<form method="post" action="/auth/verify/resend" class="auth-entry-form">`

### automation.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### backlinks.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 18 : `<form method="post" action="/projects/{{ project.slug }}/crawl" class="inline">`
- Ligne 47 : `<form method="get" class="filters">`
- Ligne 70 : `<form method="post" action="/projects/{{ project.slug }}/backlinks/import" enctype="multipart/form-data" class="filters" style="align-items:flex-end;" >`
- Ligne 128 : `<form method="post" action="/projects/{{ project.slug }}/backlinks/ahrefs/sync" class="inline">`
- Ligne 167 : `<details class="setting-details">`
- Ligne 254 : `<form method="post" action="/projects/{{ project.slug }}/backlinks/clear" style="margin-top:12px;">`

### backlinks_opportunities.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 251 : `<details>`
- Ligne 305 : `<form method="get" class="opp-filters">`
- Ligne 348 : `<form method="post" action="/projects/{{ slug }}/backlinks/opportunities/{{ opp.id }}/delete" onsubmit="return confirm({{ tr("Supprimer ?")|tojson|forceescape }});" style="margin:0;">`
- Ligne 363 : `<details style="margin-top:10px;">`
- Ligne 384 : `<details {% if auto_cfg.enabled %}open{% endif %}>`

### billing.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 85 : `<form method="post" action="/billing/portal" class="inline">`
- Ligne 91 : `<form method="post" action="/billing/portal" class="inline">`
- Ligne 127 : `<form method="post" action="/billing/cancel-scheduled-change" class="inline" style="margin-top:10px;">`
- Ligne 171 : `<form method="post" action="/billing/portal">`
- Ligne 177 : `<form method="post" action="/billing/checkout">`

### cms_settings.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 12 : `<form method="post" action="/projects/{{ slug }}/cms/platform" class="cms-form">`
- Ligne 26 : `<form method="post" action="/projects/{{ slug }}/cms/wordpress/disconnect">`
- Ligne 29 : `<details class="cms-help">`
- Ligne 30 : `<form method="post" action="/projects/{{ slug }}/cms/wordpress/connect" class="cms-form">`

### cms_wordpress_batch.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 47 : `<form method="post" action="/projects/{{ slug }}/cms/wordpress/issues/{{ issue_key }}/propose" class="inline">`
- Ligne 59 : `<form method="post" action="/projects/{{ slug }}/cms/wordpress/issues/{{ issue_key }}/apply" class="card" id="batch-review">`
- Ligne 91 : `<form method="post" action="/projects/{{ slug }}/cms/wordpress/issues/{{ issue_key }}/verify">`

### cms_wordpress_change.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 7 : `<form method="post" action="/projects/{{ slug }}/cms/wordpress/changes/{{ change.id }}/{{ action }}">`
- Ligne 59 : `<details>`
- Ligne 60 : `<details class="cms-help" id="restoration">`

### cms_wordpress_edit.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 14 : `<form method="post" action="/projects/{{ slug }}/cms/wordpress/suggest" class="cms-form" id="cms-suggest">`
- Ligne 20 : `<form method="post" action="/projects/{{ slug }}/cms/wordpress/preview" class="cms-form">`

### competitors.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 39 : `<form method="post" action="/projects/{{ slug }}/competitors/add" class="setting-form">`
- Ligne 163 : `<form method="post" action="/projects/{{ slug }}/competitors/analyze" class="inline">`
- Ligne 170 : `<form method="post" action="/projects/{{ slug }}/competitors/remove" class="inline">`

### content.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 84 : `<details class="screen-options content-style">`
- Ligne 204 : `<details class="screen-options">`
- Ligne 211 : `<details class="content-automation" {% if auto.enabled %}open{% endif %}>`

### content_valider.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 29 : `<details class="technical-details">`
- Ligne 33 : `<form method="post" action="/projects/{{ slug }}/content/valider/{{ task_id }}" class="content-approval">`

### corrections.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 448 : `<form method="post" action="/projects/{{ slug }}/crawl" class="inline">`

### crawl_settings.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 171 : `<form id="analysis-settings" method="post" action="/projects/{{ slug }}/settings/crawl">`
- Ligne 210 : `<details class="cs-options" id="exploration-options">`
- Ligne 256 : `<details class="cs-card" id="analysis-sources">`
- Ligne 608 : `<details class="cs-card" id="analysis-advanced">`
- Ligne 865 : `<details class="cs-card" id="github-repo" data-project-slug="{{ slug }}" style="margin-top:20px;">`

### crawls.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### docs_article.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### docs_index.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### error.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

- Ligne 91 : `<details class="error-desc">`

### file.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 36 : `<details class="file-preview-path">`

### first_correction.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 11 : `<form method="post" action="/projects/{{ slug }}/crawl">`
- Ligne 26 : `<details class="screen-options first-correction-retry">`
- Ligne 80 : `<form method="post" action="/projects/{{ slug }}/crawl">`
- Ligne 98 : `<form method="post" action="/projects/{{ slug }}/first-correction">`
- Ligne 123 : `<form method="post" action="/projects/{{ slug }}/first-correction">`
- Ligne 141 : `<details class="screen-options" data-correction-instructions>`
- Ligne 155 : `<details class="cms-help">`
- Ligne 166 : `<form method="post" action="/projects/{{ slug }}/first-correction">`

### home_public.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 319 : `<details>`
- Ligne 323 : `<details>`
- Ligne 327 : `<details>`
- Ligne 331 : `<details>`
- Ligne 335 : `<details>`
- Ligne 339 : `<details>`

### index.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

- Ligne 21 : `<form method="post" action="/run">`

### issue_detail.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 50 : `<details class="technical-details">`
- Ligne 82 : `<form method="post" action="/projects/{{ project.slug }}/fix-suggestions/generate" class="inline">`
- Ligne 202 : `<form method="get" class="filters" style="margin-top: 8px;">`
- Ligne 252 : `<details class="logs-details">`
- Ligne 288 : `<details class="logs-details">`

### issues.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 21 : `<form method="post" action="/projects/{{ project.slug }}/crawl" class="inline">`
- Ligne 97 : `<details class="screen-options">`
- Ligne 108 : `<form method="post" action="/projects/{{ project.slug }}/fix-suggestions/generate" class="inline">`
- Ligne 117 : `<details class="screen-options" {% if severity or category or q %}open{% endif %}>`
- Ligne 120 : `<form method="get" class="filters" style="width:100%;">`
- Ligne 195 : `<details class="technical-details">`
- Ligne 246 : `<details class="technical-details">`

### job.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 25 : `<form action="/jobs/{{ job.id }}/cancel" method="post" data-job-cancel{% if not activity.active %} hidden{% endif %}>`
- Ligne 26 : `<form action="/jobs/{{ job.id }}/retry" method="post" data-job-retry{% if job.status not in ['failed','canceled'] %} hidden{% endif %}>`
- Ligne 31 : `<details class="card screen-options" id="activity-technical">`
- Ligne 45 : `<form action="/jobs/{{ job.id }}/apply-corrections" method="post">`

### jobs.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 44 : `<form class="card activity-filters" data-activity-filters>`

### keyword_opportunities.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 17 : `<form method="get" class="result-period">`
- Ligne 82 : `<form method="post" action="/projects/{{ slug }}/keywords/track" class="inline">`
- Ligne 124 : `<form method="post" action="/projects/{{ slug }}/keywords/track" class="setting-form">`
- Ligne 168 : `<form method="post" action="/projects/{{ slug }}/keywords/untrack" class="inline">`

### keywords.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

- Ligne 18 : `<form method="post" action="/projects/{{ project.slug }}/crawl" class="inline">`
- Ligne 35 : `<form method="get" class="filters">`

### layout.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

- Ligne 85 : `<details class="nav-group" {% if '/performance' in request.url.path or '/keywords' in request.url.path or '/competitors' in request.url.path or '/backlinks' in request.url.path or '/netlinking' in request.url.path %}open{% endif %}>`
- Ligne 112 : `<details class="nav-group" {% if '/settings/search' in request.url.path or '/settings/crawl' in request.url.path or '/automation' in request.url.path or '/crawls' in request.url.path or request.url.path == project_path ~ '/cms' %}open{% endif %}>`
- Ligne 213 : `<form method="post" action="/auth/logout">`
- Ligne 260 : `<form class="assistant-form" id="assistant-form">`
- Ligne 294 : `<form action="/settings/account/country" method="post">`

### legal_notice_public.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### legal_notice_public_en.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### oauth_fragment_callback.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 41 : `<form id="oauth-complete-form" method="post" action="{{ complete_url }}">`

### performance.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 47 : `<details class="screen-options" {% if q %}open{% endif %}>`
- Ligne 50 : `<form method="get" class="filters" style="width:100%;">`

### pricing_public.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### privacy_public.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### privacy_public_en.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### project_automation.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### project_overview.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 27 : `<form method="post" action="/projects/{{ project.slug }}/crawl" class="inline">`
- Ligne 115 : `<details class="overview-score-help">`
- Ligne 153 : `<form method="post" action="/projects/{{ project.slug }}/crawl" class="inline">`
- Ligne 205 : `<details class="logs-details" id="live-job-logs-details" style="display:none;">`
- Ligne 234 : `<form method="post" action="/projects/{{ project.slug }}/crawl" class="inline">`
- Ligne 257 : `<details class="screen-options overview-report-controls">`
- Ligne 260 : `<form method="get" class="filters" style="width:100%;">`
- Ligne 317 : `<details class="overview-action-details">`
- Ligne 456 : `<details class="overview-cwv-details">`

### projects.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 83 : `<form id="batch-crawl-form" method="post" action="/projects/crawl-batch">`
- Ligne 86 : `<form id="delete-projects-form" method="post" action="/projects/delete">`
- Ligne 165 : `<details class="site-github-choice">`
- Ligne 289 : `<form method="post" action="/projects/{{ p.slug }}/crawl" class="crawl-start-form" data-slug="{{ p.slug }}">`
- Ligne 522 : `<dialog id="add-project-dialog" class="modal" aria-labelledby="site-add-title" aria-describedby="site-add-description" data-auto-open="{{ '1' if site_add.open else '0' }}">`
- Ligne 525 : `<form method="dialog">`
- Ligne 530 : `<details class="site-add-import">`
- Ligne 531 : `<form method="post" action="/projects/add" id="gsc-add-form">`

### public_layout.html

Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.

- Ligne 116 : `<form method="post" action="/auth/logout" class="inline">`

### seo_resource_article_public.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### seo_resources_public.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### settings_accounts.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 7 : `<dialog id="info-{{ item.key }}" class="modal" aria-labelledby="info-title-{{ item.key }}">`
- Ligne 10 : `<form method="dialog">`
- Ligne 83 : `<details class="setting-details" {% if item.source == "none" %}open{% endif %}>`
- Ligne 85 : `<form method="post" action="/settings/accounts" class="setting-form">`
- Ligne 159 : `<form method="post" action="/settings/account/preferences" style="margin-top:16px;">`
- Ligne 265 : `<form method="post" action="/settings/account/attribution/withdraw">`
- Ligne 282 : `<details class="setting-details">`
- Ligne 284 : `<form method="post" action="/settings/account/password" class="setting-form">`
- Ligne 319 : `<details class="setting-details">`
- Ligne 321 : `<form method="post" action="/settings/account/delete" class="setting-form">`
- Ligne 408 : `<form method="post" action="/settings/accounts/gsc/disconnect-all" style="display:inline-flex;">`
- Ligne 436 : `<form method="post" action="{{ p.disconnect_url }}" style="display:contents;">`
- Ligne 555 : `<form method="post" action="{{ github_oauth.disconnect_url }}" style="display:inline-flex;">`
- Ligne 569 : `<details class="setting-details">`
- Ligne 571 : `<form method="post" action="/settings/accounts" class="setting-form">`
- Ligne 672 : `<form method="post" action="{{ bing_oauth.disconnect_url }}" style="display:inline-flex;">`
- Ligne 714 : `<details class="setting-details" {% if bing_oauth.source == "none" and not bing_oauth.ready %}open{% endif %}>`
- Ligne 716 : `<form method="post" action="/settings/accounts" class="setting-form">`
- Ligne 835 : `<form method="post" action="{{ netlify_oauth.disconnect_url }}" style="display:inline-flex;">`
- Ligne 849 : `<details class="setting-details">`
- Ligne 851 : `<form method="post" action="/settings/accounts" class="setting-form">`
- Ligne 928 : `<form method="post" action="/projects/add" style="display:inline-flex; gap:8px; align-items:center;">`

### settings_activation.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 17 : `<form method="get" action="/settings/activation#active-plans" data-plan-refresh-form>`
- Ligne 36 : `<details class="live-plans-method">`
- Ligne 39 : `<form id="first-results" class="activation-period card" method="get" action="/settings/activation">`
- Ligne 72 : `<details class="card campaign-links" id="campaign-links" {% if not campaigns or err or msg %}open{% endif %}>`
- Ligne 75 : `<form method="post" action="/settings/activation/campaigns" class="campaign-form">`
- Ligne 87 : `<details class="card activation-method">`

### settings_customer_account_profile.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 42 : `<details class="profile-extra">`
- Ligne 46 : `<details>`

### settings_customer_accounts.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 17 : `<form class="card account-filters" action="/settings/activation/accounts" method="get">`
- Ligne 33 : `<details>`
- Ligne 46 : `<details class="card account-method">`

### settings_legal_notices.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 19 : `<form method="post" action="/settings/legal-notices" class="stack">`
- Ligne 58 : `<form method="post" action="/settings/legal-notices/{{ notice.id }}/resume">`

### settings_operations.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 63 : `<form method="get" action="/settings/operations" class="inline" style="display:flex;gap:6px;align-items:center;">`

### settings_search.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 29 : `<details>`

### settings_system.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 43 : `<details class="setting-details">`
- Ligne 45 : `<form method="post" action="{{ action }}" class="setting-form">`
- Ligne 69 : `<dialog id="info-{{ item.key }}" class="modal">`
- Ligne 72 : `<form method="dialog">`
- Ligne 189 : `<form method="post" action="/settings/system/secrets/rotate" class="setting-form" style="margin-top:0;">`
- Ligne 247 : `<details class="setting-details" {% if not (cred and cred.exists) %}open{% endif %}>`
- Ligne 249 : `<form method="post" action="/settings/system" id="gsc-form" class="setting-form">`

### settings_team.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

- Ligne 21 : `<form method="post" action="/auth/logout">`
- Ligne 38 : `<form method="post" action="/settings/team/leave" onsubmit="return confirm({{ tr('Quitter ce compte partagé ? Vous perdrez l’accès à ses sites.')|tojson|forceescape }});">`
- Ligne 72 : `<form method="post" action="/settings/team/member/{{ m.user_id }}/remove" onsubmit="return confirm({{ tr('Retirer cette personne ? Elle perdra l’accès à vos sites.')|tojson|forceescape }});">`
- Ligne 99 : `<form method="post" action="/settings/team/invite/{{ i.id }}/revoke">`
- Ligne 115 : `<form method="post" action="/settings/team/invite" class="team-invite">`

### status_public.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### support_public.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### terms_public.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

Aucune balise de formulaire, fenêtre ou panneau repliable.

### terms_public_en.html

Référence repérée dans un gestionnaire de route ou sa variante juridique.

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
| `/docs/corriger-dans-wordpress` | [corriger-dans-wordpress.md](../content/docs/corriger-dans-wordpress.md) |
| `/docs/demarrer` | [demarrer.md](../content/docs/demarrer.md) |
| `/docs/depannage` | [depannage.md](../content/docs/depannage.md) |
| `/en/docs/ajouter-un-projet` | [ajouter-un-projet.md](../content/docs/en/ajouter-un-projet.md) |
| `/en/docs/analyser-les-concurrents` | [analyser-les-concurrents.md](../content/docs/en/analyser-les-concurrents.md) |
| `/en/docs/anomalies-et-priorites` | [anomalies-et-priorites.md](../content/docs/en/anomalies-et-priorites.md) |
| `/en/docs/automatiser-les-audits` | [automatiser-les-audits.md](../content/docs/en/automatiser-les-audits.md) |
| `/en/docs/backlinks` | [backlinks.md](../content/docs/en/backlinks.md) |
| `/en/docs/comptes-d-equipe` | [comptes-d-equipe.md](../content/docs/en/comptes-d-equipe.md) |
| `/en/docs/connecter-github` | [connecter-github.md](../content/docs/en/connecter-github.md) |
| `/en/docs/connecter-search-console` | [connecter-search-console.md](../content/docs/en/connecter-search-console.md) |
| `/en/docs/corrections-automatiques` | [corrections-automatiques.md](../content/docs/en/corrections-automatiques.md) |
| `/en/docs/corriger-dans-wordpress` | [corriger-dans-wordpress.md](../content/docs/en/corriger-dans-wordpress.md) |
| `/en/docs/demarrer` | [demarrer.md](../content/docs/en/demarrer.md) |
| `/en/docs/depannage` | [depannage.md](../content/docs/en/depannage.md) |
| `/en/docs/exports-et-rapports` | [exports-et-rapports.md](../content/docs/en/exports-et-rapports.md) |
| `/en/docs/fix-pack-sans-depot` | [fix-pack-sans-depot.md](../content/docs/en/fix-pack-sans-depot.md) |
| `/en/docs/lancer-un-crawl` | [lancer-un-crawl.md](../content/docs/en/lancer-un-crawl.md) |
| `/en/docs/lire-le-rapport` | [lire-le-rapport.md](../content/docs/en/lire-le-rapport.md) |
| `/en/docs/opportunites-de-mots-cles` | [opportunites-de-mots-cles.md](../content/docs/en/opportunites-de-mots-cles.md) |
| `/en/docs/plans-et-quotas` | [plans-et-quotas.md](../content/docs/en/plans-et-quotas.md) |
| `/en/docs/rediger-du-contenu` | [rediger-du-contenu.md](../content/docs/en/rediger-du-contenu.md) |
| `/en/docs/suivre-la-performance` | [suivre-la-performance.md](../content/docs/en/suivre-la-performance.md) |
| `/en/docs/verifier-une-correction` | [verifier-une-correction.md](../content/docs/en/verifier-une-correction.md) |
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
| `/en/ressources-seo/audit-seo-technique-checklist-priorites` | [audit-seo-technique-checklist-priorites.md](../content/blog/en/audit-seo-technique-checklist-priorites.md) |
| `/en/ressources-seo/ce-que-vos-concurrents-publient-et-pas-vous` | [ce-que-vos-concurrents-publient-et-pas-vous.md](../content/blog/en/ce-que-vos-concurrents-publient-et-pas-vous.md) |
| `/en/ressources-seo/connecter-google-search-console-audit-mensuel` | [connecter-google-search-console-audit-mensuel.md](../content/blog/en/connecter-google-search-console-audit-mensuel.md) |
| `/en/ressources-seo/core-web-vitals-lire-signaux-seo` | [core-web-vitals-lire-signaux-seo.md](../content/blog/en/core-web-vitals-lire-signaux-seo.md) |
| `/en/ressources-seo/corriger-le-seo-dans-le-code-pas-dans-un-rapport` | [corriger-le-seo-dans-le-code-pas-dans-un-rapport.md](../content/blog/en/corriger-le-seo-dans-le-code-pas-dans-un-rapport.md) |
| `/en/ressources-seo/corriger-title-meta-description-grande-echelle` | [corriger-title-meta-description-grande-echelle.md](../content/blog/en/corriger-title-meta-description-grande-echelle.md) |
| `/en/ressources-seo/frequence-crawl-seo-site-vitrine-ecommerce-blog` | [frequence-crawl-seo-site-vitrine-ecommerce-blog.md](../content/blog/en/frequence-crawl-seo-site-vitrine-ecommerce-blog.md) |
| `/en/ressources-seo/netlinking-opportunites-backlinks-sans-spam` | [netlinking-opportunites-backlinks-sans-spam.md](../content/blog/en/netlinking-opportunites-backlinks-sans-spam.md) |
| `/en/ressources-seo/regression-seo-apres-mise-en-production` | [regression-seo-apres-mise-en-production.md](../content/blog/en/regression-seo-apres-mise-en-production.md) |
| `/ressources-seo/frequence-crawl-seo-site-vitrine-ecommerce-blog` | [frequence-crawl-seo-site-vitrine-ecommerce-blog.md](../content/blog/frequence-crawl-seo-site-vitrine-ecommerce-blog.md) |
| `/ressources-seo/netlinking-opportunites-backlinks-sans-spam` | [netlinking-opportunites-backlinks-sans-spam.md](../content/blog/netlinking-opportunites-backlinks-sans-spam.md) |
| `/ressources-seo/regression-seo-apres-mise-en-production` | [regression-seo-apres-mise-en-production.md](../content/blog/regression-seo-apres-mise-en-production.md) |
