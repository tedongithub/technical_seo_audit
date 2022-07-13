SELECT
site,
domain,
search_console_account,
google_analytics_account
FROM (

	SELECT
	distinct(client_name) site,
	trim(replace(replace(replace(domain,'www.',''),'http://',''),'https://',''),'/') domain,
	domain search_console_account,
	case when data_source = 'Google Analytics' then account end google_analytics_account,
	og_time_of_entry time_of_entry,
	first_value(og_time_of_entry) OVER (PARTITION BY client_name ORDER BY og_time_of_entry DESC) lv
	FROM `seo-data-pipeline-89.bq_sources.accounts`
	WHERE client_name is not null
)
WHERE lv = time_of_entry AND google_analytics_account is not null
group by site, domain, search_console_account, google_analytics_account