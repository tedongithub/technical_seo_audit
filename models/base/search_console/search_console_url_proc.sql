SELECT
b.site, 
a.domain,
a.account,
date,
unix_date,
date_of_entry,
url_untrimmed,
first_value(url_untrimmed) over (PARTITION BY site, a.domain, account, date, url_trimmed ORDER BY impressions desc) url,
impressions,
clicks,
average_position
FROM (

	SELECT  
	DISTINCT(site_url) as account,
    date_trunc(cast(date as date), month) as date,
    unix_date(cast(date_trunc(cast(date as date), month) as date)) unix_date,
    _sdc_received_at time_of_entry,
    cast(_sdc_received_at as date) date_of_entry,
	first_value(_sdc_received_at) OVER (PARTITION BY site_url, page, date_trunc(cast(date as date), month) ORDER BY _sdc_received_at desc) lv,	
	--is this where the month column is getting lost?     --site_url as account,  --requested_object as account,
	lower(regexp_replace(replace(replace(replace(page,'www.',''),'http://',''),'https://',''),r'\#.*$','')) url_untrimmed,
	trim(lower(regexp_replace(replace(replace(replace(page,'www.',''),'http://',''),'https://',''),r'\#.*$','')),'/') url_trimmed,
	regexp_extract(page,r'^(?:https?:\/\/)?(?:www\.)?([^\/]+)') as domain,
	impressions,
	clicks,
	position average_position
	FROM     {{ source('bq_sources', 'gsc') }}
	) a

LEFT JOIN {{ ref('domains_proc') }} b
ON (
	a.account = b.search_console_account
)
WHERE time_of_entry = lv


    


