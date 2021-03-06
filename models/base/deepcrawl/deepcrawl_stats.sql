SELECT 
a.crawl_id,
a.site,
b.run_date report_date,
a.crawl_datetime,
a.crawl_date,
domain,
domain_canonical,
url,
url_stripped,
canonical_url,
canonical_status,
url_protocol,
canonical_url_protocol,
protocol_match,
protocol_count,
path_count,
first_path,
second_path,
last_path,
filename,
last_subfolder,
last_subfolder_canonical,
first_subfolder,
second_subfolder,
urls_to_canonical,
found_at_sitemap,
http_status_code,
level,
schema_type,
header_content_type,
word_count, 
med_word_count,
replace(replace(page_title,'(',''),')','') page_title,
page_title_length,
replace(replace(description,'(',''),')','') description,
description_length,
indexable,
robots_noindex,
meta_noindex,
is_self_canonical,
backlink_count,
backlink_domain_count,
med_ref_domain_count,
bottom_quartile_ref_domain_count,
redirected_to_url,
found_at_url,
rel_next_url,
rel_prev_url,
links_in_count,
bottom_quartile_internal_links_in_count,
top_quartile_internal_links_in_count,
links_out_count,
bottom_quartile_internal_links_out_count,
external_links_count,
internal_links_count,
h1_tag,
h2_tag,
redirect_chain,
redirected_to_status_code,
is_redirect_loop,
duplicate_page,
duplicate_page_count,
duplicate_body,
duplicate_body_count,
class_path,
class_sitemap,
class_schema,
flag_google_maps,
flag_blog_path,
flag_blog_h1,
flag_high_word_count,
flag_thin_page,
flag_reviews,
flag_select_size,
flag_add_to_cart,
flag_prices,
flag_above_avg_prices,
flag_form_submit,
flag_learn_more,
flag_info_path,
flag_paginated,
case 
	WHEN http_status_code in (403, 404) THEN '' 	
	when first_path = '' then 'homepage'
  	when class_schema = 'category' then class_schema
	when class_sitemap in ('page','none','') and class_path in ('blog','category','author','event','local','info','product') then class_path
	when class_sitemap in ('page','none','') and class_schema is not null then class_schema
	when class_sitemap != 'page' and class_sitemap is not null then class_sitemap
	when class_path != '' then class_path
	else class_sitemap end as page_type

-- CASE WHEN http_status_code in (403, 404) THEN '' 
-- 	WHEN url = domain THEN 'homepage'
-- 	WHEN url like '%404%' THEN '404'
-- 	WHEN class_schema is not null THEN class_schema 
-- 	WHEN class_sitemap is not null THEN class_sitemap
-- 	WHEN flag_learn_more = 1 OR flag_paginated = 1 OR url like '%/category/%' THEN 'category'
-- 	WHEN (flag_reviews + flag_select_size + flag_add_to_cart + flag_prices) >= 2 THEN 'product'
-- 	WHEN flag_google_maps = 1 THEN 'local'
-- 	WHEN flag_form_submit = 1 THEN 'lead generation'
-- 	WHEN flag_thin_page = 1 THEN 'thin content'
-- 	ELSE 'general content' END as page_type 
FROM 
(
	SELECT 
	crawl_id,
	site,
	crawl_datetime,
	crawl_date,
	crawl_month,
	crawl_report_month,
	domain,
	domain_canonical,
	url,
	url_stripped,
	canonical_url,
	canonical_status,
	url_protocol,
	canonical_url_protocol,
	protocol_match,
	protocol_count,
	path_count,
	first_path,
	second_path,
	last_path,
	filename,
	last_subfolder,
	last_subfolder_canonical,
	first_subfolder,
	second_subfolder,
	urls_to_canonical,
	found_at_sitemap,
	http_status_code,
	level,
	schema_type,
	header_content_type,
	word_count, 
	med_word_count,
	page_title,
	page_title_length,
	description,
	description_length,
	indexable,
	robots_noindex,
	meta_noindex,
	is_self_canonical,
	backlink_count,
	backlink_domain_count,
	PERCENTILE_DISC(backlink_domain_count, 0.5 IGNORE NULLS) OVER w1 AS med_ref_domain_count,
	PERCENTILE_DISC(backlink_domain_count, 0.25 IGNORE NULLS) OVER w1 AS bottom_quartile_ref_domain_count,
	redirected_to_url,
	found_at_url,
	rel_next_url,
	rel_prev_url,
  	links_in_count,
  	PERCENTILE_DISC(links_in_count, 0.25 IGNORE NULLS) OVER w1 AS bottom_quartile_internal_links_in_count,
	PERCENTILE_DISC(links_in_count, 0.75 IGNORE NULLS) OVER w1 AS top_quartile_internal_links_in_count,
  	links_out_count,
  	PERCENTILE_DISC(internal_links_count, 0.25 IGNORE NULLS) OVER w1 AS bottom_quartile_internal_links_out_count,
  	external_links_count,
  	internal_links_count,	
	h1_tag,
	h2_tag,
	redirect_chain,
	redirected_to_status_code,
	is_redirect_loop,
	duplicate_page,
	duplicate_page_count,
	duplicate_body,
	duplicate_body_count,
	class_path,
	class_sitemap,
	class_schema,
	flag_google_maps,
	flag_blog_path,
	flag_blog_h1,
	flag_high_word_count,
	flag_thin_page,
	flag_reviews,
	flag_select_size,
	flag_add_to_cart,
	flag_prices,
	flag_above_avg_prices,
	flag_learn_more,
	flag_info_path,
	flag_form_submit,
	flag_paginated
	-- (flag_reviews + flag_select_size + flag_add_to_cart + flag_prices) as product_score,
	-- (flag_reviews + flag_select_size + flag_add_to_cart + flag_above_avg_prices) as category_score,
	-- (flag_blog_path + flag_blog_h1 + flag_high_word_count) as article_score
	FROM {{ref('deepcrawl_url_proc')}}
	WINDOW w1 as (PARTITION BY domain, crawl_month)
) a
LEFT JOIN {{ ref('crawl_dates') }} b
ON (
	a.crawl_id = b.crawl_id AND
	a.site = b.site
)
WHERE (found_at_url is not null OR found_at_sitemap is not null)



