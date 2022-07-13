select
crawl_id,
site,
domain,
domain_canonical,
crawl_datetime,
crawl_date,
crawl_month,
crawl_report_month,
url,
url_stripped,
canonical_url,
canonical_status,
path_count,
ifnull(first_path, '') first_path,
ifnull(second_path, '') second_path,
ifnull(last_path, '') last_path,
ifnull(split(split(last_path,'.')[SAFE_ORDINAL(2)],'?')[SAFE_ORDINAL(1)],'') as filename,
trim(replace(url, last_path, ''),'/') last_subfolder,
trim(replace(canonical_url, last_path, ''),'/') last_subfolder_canonical,
case when trim(replace(url, last_path, ''),'/') = domain then domain else concat(domain,'/',first_path) end as first_subfolder,
case when trim(replace(url, last_path, ''),'/') = domain then domain 
  when second_path is not null then concat(domain,'/',first_path,'/',second_path) 
  else '' end as second_subfolder,
urls_to_canonical,
url_protocol,
canonical_url_protocol,
case when url_protocol = canonical_url_protocol then 1 else 0 end as protocol_match,
count(distinct(url_protocol)) over (partition by url) protocol_count,
found_at_sitemap,
http_status_code,
level,
schema_type,
header_content_type,
word_count, 
med_word_count,
lower(page_title) page_title,
page_title_length,
lower(description) description,
description_length,
indexable,
robots_noindex,
meta_noindex,
is_self_canonical,
backlink_count,
backlink_domain_count,
redirected_to_url,
found_at_url,
rel_next_url,
rel_prev_url,
links_in_count,
links_out_count,
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
case 
  when first_path like '%category%' then 'category'
  when first_path like '%blog%' or first_path like '%resource%' or first_path like '%article%' then 'blog'
  when first_path like '%product%' then 'product'
  when first_path like '%author%' then 'author'
  when first_path like '%location%' or first_path like '%stores%' then 'local'
  when regexp_contains(first_path, r'sitemap|cart|disclaimer|customer-service|returns|faq|warranty|affiliate|loyalty|register|wholesale|account|wishlist|jobs|password|contact|login|signup|privacy|about') then 'info'
  when second_path = '' or second_path is null then 'page'
  else first_path end as class_path,
case when found_at_sitemap is null then null
  when found_at_sitemap like '%category%' then 'category'
  when found_at_sitemap like '%product%' then 'product'
  when found_at_sitemap like '%author%' then 'author'  
  when found_at_sitemap like '%post%' then 'blog'
  when found_at_sitemap like '%location%' then 'local'
  when found_at_sitemap like '%page%' then 'page'
  else null end as class_sitemap,
case when schema_type like '%product%' then 'product'
  when schema_type like '%collection%' or schema_type like '%category%' then 'category'
  when schema_type like '%author%' then 'author'  
  when ( schema_type like '%creativework%' or schema_type like '%blog%' or schema_type like '%article%') then 'blog'
  when schema_type like '%event%' then 'event'
  when schema_type like '%local%' or schema_type like '%place%' then 'local'
  else null end as class_schema,
case when qt_google_maps > min_google_maps then 1 else 0 end as flag_google_maps,
case when qt_cur_price > min_cur_price then 1 else 0 end as flag_prices,
case when qt_cur_price > avg_cur_price then 1 else 0 end as flag_above_avg_prices,
case when qt_cur_price = min_cur_price then 1 else 0 end as flag_min_prices,
case when qt_add_to_cart > min_add_to_cart then 1 else 0 end as flag_add_to_cart,
case when qt_reviews > min_reviews then 1 else 0 end as flag_reviews,
case when qt_size > min_size then 1 else 0 end as flag_select_size,
case when qt_learn_more > med_learn_more then 1 else 0 end as flag_learn_more,
case when med_word_count > word_count then 1 else 0 end as flag_high_word_count,
case when word_count < 500 then 1 else 0 end as flag_thin_page,
case when qt_form_submit > min_form_submit then 1 else 0 end as flag_form_submit,
case when regexp_contains(url, r'/blog|blog.|resource|article|knowledge') then 1 else 0 end as flag_blog_path,
case when regexp_contains(h1_tag, r'/blog|blog.|resource|article|knowledge') then 1 else 0 end as flag_blog_h1,
case when regexp_contains(url, r'sitemap|customer-service|returns|affiliate|loyalty|register|wholesale|about-us|help|account|wishlist|jobs|password|contact|stores|login|signup') then 1 else 0 end as flag_info_path,
case when rel_next_url is not null or rel_prev_url is not null or paginated_page = true then 1 else 0 end as flag_paginated 

FROM 
 (
  SELECT
  crawl_id,
  site,
  domain,
  domain_canonical,
  crawl_datetime,
  extract(date from crawl_datetime) crawl_date, 
  extract(month from crawl_datetime) crawl_month,
    extract(month from crawl_datetime) crawl_report_month,
  url,
  url_stripped,
  canonical_url,
  ARRAY_LENGTH(SPLIT(url, '/')) path_count,
  SPLIT(url, '/')[SAFE_ORDINAL(2)] first_path,
  SPLIT(url, '/')[SAFE_ORDINAL(3)] second_path,
  ARRAY_REVERSE(SPLIT(url, '/'))[SAFE_ORDINAL(1)] last_path,
  ARRAY_REVERSE(SPLIT(canonical_url, '/'))[SAFE_ORDINAL(1)] last_path_canonical,
  urls_to_canonical,
  case when url = canonical_url then 'self'
    when canonical_url = '' or canonical_url is null then 'missing_canonical'
    else 'canonicalized' end as canonical_status,
  url_protocol,
  canonical_url_protocol,
  found_at_sitemap,
  http_status_code,
  level,
  schema_type,
  header_content_type,
  word_count, 
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
  redirected_to_url,
  found_at_url,
  rel_next_url,
  rel_prev_url,
  links_in_count,
  links_out_count,
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
  PERCENTILE_DISC(word_count, 0.5 RESPECT NULLS) OVER w2 AS med_word_count,
  qt_dec_price,
  min(qt_dec_price) OVER w1 AS min_dec_price,
  avg(qt_dec_price) OVER w1 AS avg_dec_price,
  PERCENTILE_DISC(qt_dec_price, 0.5 RESPECT NULLS) OVER w2 AS med_dec_price,
  qt_cur_price,
  min(qt_cur_price) OVER w1 AS min_cur_price,
  avg(qt_cur_price) OVER w1 AS avg_cur_price,
  PERCENTILE_DISC(qt_cur_price, 0.5 RESPECT NULLS) OVER w2 AS med_cur_price,
  qt_add_to_cart,
  min(qt_add_to_cart) OVER w1 AS min_add_to_cart,
  qt_reviews,
  min(qt_reviews) OVER w1 AS min_reviews,
  qt_size,
  min(qt_size) OVER w1 AS min_size,
  qt_google_maps,
  min(qt_google_maps) OVER w1 AS min_google_maps,
  qt_learn_more,
  PERCENTILE_CONT(qt_learn_more, .5) OVER w2 AS med_learn_more,
  qt_form_submit,
  min(qt_form_submit) OVER w1 AS min_form_submit,
  qt_infinite_scroll,
  paginated_page
  FROM {{ ref('deepcrawl_proc') }} 
  WINDOW w1 as (PARTITION BY domain, extract(date from crawl_datetime), header_content_type, http_status_code ORDER BY domain desc),
  w2 as (PARTITION BY domain, extract(date from crawl_datetime), header_content_type, http_status_code)

  )

  