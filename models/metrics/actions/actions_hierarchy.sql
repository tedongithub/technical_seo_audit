SELECT
date,
crawl_date,
site,
domain,
url,
found_at,
found_at_sitemap,
found_at_url,
canonical_url,
canonical_status,
page_type,
CASE WHEN sessions_30d = sessions_ttm AND sessions_30d > 0 and http_status_code=200 THEN 1
	ELSE 0 END as new_content_flag,
CASE 
	WHEN lower(http_status_action) like '%redirect%' THEN 'High'
	WHEN lower(crawl_action) like '%missing from crawl%' THEN 'High'
	WHEN lower(crawl_action) like '%potential noindex%' THEN 'Medium'	
	WHEN lower(http_status_action) like '%remove internal link%' THEN 'High'	
	WHEN lower(sitemap_action) like '%remove from sitemap%' or ( lower(sitemap_action) = 'likely add to sitemap' and pct_of_organic_sessions_30d > .05 )  THEN 'High'
	WHEN lower(sitemap_action) = 'likely add to sitemap' THEN 'Medium'
	WHEN lower(canonical_action) in ('missing canonical', 'self-canonicalize, paginated page') THEN 'High'
	ELSE '' END as admin_action_priority,
CASE WHEN impressions_30d >= top_10pct_impressions_30d THEN 'High'
	WHEN impressions_30d >= med_impressions_30d THEN 'Medium'
	ELSE 'Low' END as mktg_action_priority,	
case 
	when http_status_action != '' then http_status_action
	when crawl_action != '' then crawl_action
	when lower(sitemap_action) not in ('leave as is', '') then sitemap_action
	when lower(canonical_action) not in ('leave as is', '') then canonical_action
	else '' end as top_admin_action,
case 
	when http_status_action != '' then concat(cast(http_status_code as string),' page status')
	when http_status_code is null and lower(crawl_action) like '%missing%' then 'not crawled by deepcrawl'
	when lower(crawl_action) like '%removed%' then 'not crawled, 0 traffic'
	when crawl_action != '' then '0 traffic'
	when lower(sitemap_action) like '%remove%' then '301, 404, noindexed or thin page'
	when sitemap_action != '' then 'page missing from sitemap'
	when lower(canonical_action) = 'missing canonical' then 'canonical url not found in crawl'
	when lower(crawl_action) like 'block crawl to:%' then 'subfolder receives no traffic'
	when lower(crawl_action) = 'noindex' then 'page receives no organic traffic'
	else '' end as top_admin_action_reason,
crawl_action,
CASE 
	WHEN lower(crawl_action) like '%missing from crawl%' THEN 'High'
	WHEN lower(crawl_action) like '%potential noindex%' THEN 'Medium'	
	ELSE '' END as crawl_action_priority,
http_status_action,
CASE WHEN lower(http_status_action) like '%redirect%' THEN 'High' ELSE '' END as http_status_action_priority,
sitemap_action,
CASE 
	WHEN lower(sitemap_action) like '%remove from sitemap%' or ( sitemap_action = 'likely add to sitemap' and pct_of_organic_sessions_30d > .05 )  THEN 'High'
	WHEN lower(sitemap_action) = 'likely add to sitemap' THEN 'Medium'
	ELSE '' END as sitemap_action_priority,
canonical_action,
CASE WHEN lower(canonical_action) in ('missing canonical', 'self-canonicalize, paginated page') THEN 'High' ELSE '' END as canonical_action_priority,
schema_action,
-- concat(content_action,meta_rewrite_action,pagination_action,external_link_action,schema_action) on_off_page_action,
concat(
	CASE WHEN content_action = '' THEN '' ELSE concat("content_action: ", content_action) END,
	CASE WHEN meta_rewrite_action = '' THEN '' ELSE concat("meta_rewrite_action: ", meta_rewrite_action) END,
	CASE WHEN external_link_action = '' THEN '' ELSE concat("external_link_action: ", external_link_action) END,
	CASE WHEN schema_action = '' THEN '' ELSE concat("schema_action: ", schema_action) END ) on_off_page_action,
concat(
	CASE WHEN internal_link_action = '' THEN '' ELSE concat("internal_link_action: ", internal_link_action) END,
	CASE WHEN category_action = '' THEN '' ELSE concat("category_action: ", category_action) END,
	CASE WHEN cannibalization_action = '' THEN '' ELSE concat("cannibalization_action: ", cannibalization_action) END ) architecture_action,
# analytics actions are separate from indicative actions - only display if admin_action in ('', 'add to sitemap', 'missing from crawl')
cannibalization_action,
content_trajectory,
content_action,
case when http_status_code = 200 then internal_link_action else '' end as internal_link_action,
case when http_status_code = 200 then external_link_action else '' end as external_link_action,
case when http_status_code = 200 then meta_rewrite_action else '' end as meta_rewrite_action,
case when http_status_code = 200 then category_action else '' end as category_action,
case when http_status_code = 200 then pagination_action else '' end as pagination_action,
url_protocol,
canonical_url_protocol,
protocol_match,
protocol_count,
first_subfolder,
second_subfolder,
last_subfolder,
http_status_code,
level,
schema_type,
header_content_type,
word_count, 
page_title,
title_contains_top_keyword,
page_title_length,
description,
description_contains_top_keyword,
description_length,
robots_noindex,
meta_noindex,
is_noindex,
redirected_to_url,
h1_tag,
h2_tag,
redirect_chain,
redirected_to_status_code,
is_redirect_loop,
duplicate_page,
duplicate_page_count,
duplicate_body,
duplicate_body_count,
sessions_30d,
pct_of_organic_sessions_30d,
transaction_revenue_30d,
transactions_30d,
pct_of_organic_transactions_30d,
ecommerce_conversion_rate_30d,
med_transaction_conversion_rate_30d,
goal_completions_all_goals_30d,
pct_of_organic_goal_completions_all_goals_30d,
goal_conversion_rate_all_goals_30d,
med_goal_conversion_rate_30d,
blended_conversions_30d,	
blended_conversion_rate_30d,
bounce_rate_30d,
avg_seconds_on_site_30d,
sessions_mom,
sessions_mom_pct,
transaction_revenue_mom,
transaction_revenue_mom_pct,
transactions_mom,
transactions_mom_pct,
goal_completions_all_goals_mom,
goal_completions_all_goals_mom_pct,
sessions_yoy,
sessions_yoy_pct,
transaction_revenue_yoy,
transaction_revenue_yoy_pct,
transactions_yoy,
transactions_yoy_pct,
goal_completions_all_goals_yoy,
goal_completions_all_goals_yoy_pct,
sessions_ttm,
transaction_revenue_ttm,
transactions_ttm,
ecommerce_conversion_rate_ttm,
goal_completions_all_goals_ttm,
goal_conversion_rate_all_goals_ttm,
bounce_rate_ttm,
avg_seconds_on_site_ttm,
gaining_traffic_mom,
gaining_traffic_yoy,
backlink_count,
ref_domain_count,
med_ref_domain_count,
internal_links_in_count,
internal_links_out_count,
impressions_30d,
pct_of_total_impressions_30d,
med_impressions_30d,
top_10pct_impressions_30d,
clicks_30d,
ctr_30d,
avg_position_30d,
impressions_mom,
impressions_mom_pct,
clicks_mom,
clicks_mom_pct,
ctr_mom,
ctr_mom_pct,
avg_position_mom,
impressions_yoy,
impressions_yoy_pct,
clicks_yoy,
ctr_yoy,	
ctr_yoy_pct,
avg_position_yoy,	
impressions_ttm,
clicks_ttm,
ctr_ttm,
avg_position_ttm,
top_3_keywords,
top_5_keywords,
top_10_keywords,
top_20_keywords,
main_keyword,
main_impressions,
main_clicks,
main_avg_position,
main_top_url,
main_keyword_cannibalization_flag,
main_top_url_clicks,
best_keyword,
best_impressions,
best_clicks,
best_avg_position,
best_top_url,
best_top_url_clicks,
best_keyword_cannibalization_flag
FROM {{ ref('actions_proc') }}


