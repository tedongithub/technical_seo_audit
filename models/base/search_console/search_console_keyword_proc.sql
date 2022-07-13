SELECT 
b.site, 
a.domain,
a.account,
date,
unix_date,
date_of_entry,
url_untrimmed,
first_value(url_untrimmed) over (PARTITION BY site, a.domain, account, date, url_trimmed ORDER BY impressions desc) url,
keyword,
CASE WHEN regexp_contains(keyword, lower(site)) = TRUE THEN 1 ELSE 0 END as branded_flag,
impressions,
clicks,
average_position,
CASE WHEN average_position <= 3 THEN 1 ELSE 0 END as top_3_keywords,
CASE WHEN average_position <= 5 THEN 1 ELSE 0 END as top_5_keywords,
CASE WHEN average_position <= 10 THEN 1 ELSE 0 END as top_10_keywords,
CASE WHEN average_position <= 20 THEN 1 ELSE 0 END as top_20_keywords
FROM (

	SELECT
	date,
	unix_date,
	time_of_entry,
	date_of_entry,
	account,
	lower(regexp_replace(replace(replace(replace(landing_page,'www.',''),'http://',''),'https://',''),r'\#.*$','')) url_untrimmed,
	trim(lower(regexp_replace(replace(replace(replace(landing_page,'www.',''),'http://',''),'https://',''),r'\#.*$','')),'/') url_trimmed,
	regexp_extract(landing_page,r'^(?:https?:\/\/)?(?:www\.)?([^\/]+)') as domain,
	regexp_replace(keyword, r'[^a-zA-Z]'," ") as keyword,
	impressions,
	clicks,
	average_position
	FROM (

		SELECT 
		-- as account? for now we are using site_url from the gsc_keywords source bc it is present in all rows.
		site_url as account, 
        date_trunc(cast(date as date), month) as date,
        unix_date(cast(date_trunc(cast(date as date), month) as date)) as unix_date,
		_sdc_received_at as time_of_entry,
		cast(_sdc_received_at as date) date_of_entry,
		first_value(_sdc_received_at) OVER (PARTITION BY site_url, page, query, date_trunc(cast(date as date), month) ORDER BY _sdc_received_at desc) lv,
		page as landing_page,
		query as keyword,
		impressions,
		clicks,
		position as average_position
		FROM {{ source('bq_sources', 'gsc_keywords') }} 
		)
	WHERE time_of_entry = lv
	) a
LEFT JOIN {{ ref('domains_proc') }} b
ON (
	a.account = b.search_console_account
)




