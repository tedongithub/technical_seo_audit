SELECT 
site,
domain,
account,
date,
unix_date,
date_of_entry,
url,
sum(sessions) sessions,
sum(transaction_revenue) transaction_revenue,
sum(transactions) transactions,
sum(goal_completions_all_goals) goal_completions_all_goals,
sum(bounces) bounces,
sum(seconds_on_site) seconds_on_site
FROM (

    SELECT
    site,
    domain,
    account,
    date,
    unix_date,
    date_of_entry,
    url_untrimmed,
    first_value(url_untrimmed) over (PARTITION BY site, domain, account, date, url_trimmed ORDER BY sessions desc) url,
    replace(regexp_extract(url_trimmed,r'^(?:https?:\/\/)?(?:www\.)?([^\/]+)'),'(not set)','') as url_domain,
    sessions,
    transaction_revenue,
    transactions,
    goal_completions_all_goals,
    bounces,
    seconds_on_site
    FROM (

        SELECT
        b.site, 
        a.domain,
        account,
        month date, 
        unix_date,
        date_of_entry,
        CASE WHEN regexp_contains(landing_page_path,a.domain) 
          THEN lower(regexp_replace(replace(replace(replace(landing_page_path,'www.',''),'http://',''),'https://',''),r'\#.*$',''))
          ELSE lower(regexp_replace(replace(replace(replace(CONCAT(a.domain,landing_page_path),'www.',''),'http://',''),'https://',''),r'\#.*$',''))
          END as url_untrimmed,
        CASE WHEN regexp_contains(landing_page_path,a.domain) 
          THEN trim(lower(regexp_replace(replace(replace(replace(landing_page_path,'www.',''),'http://',''),'https://',''),r'\#.*$','')),'/')
          ELSE trim(lower(regexp_replace(replace(replace(replace(CONCAT(a.domain,landing_page_path),'www.',''),'http://',''),'https://',''),r'\#.*$','')),'/')
          END as url_trimmed,          
        sum(sessions) sessions,
        sum(transaction_revenue) transaction_revenue,
        sum(transactions) transactions,
        sum(goal_completions_all_goals) goal_completions_all_goals,
        sum(bounces) bounces,
        sum(seconds_on_site) seconds_on_site
        FROM (
                SELECT 
	              -- extract(date from ga_date) date, 
                rtrim(web_property_id) as account,
                date_trunc(cast(ga_date as date), month) as month,
                -- date_trunc(ga_date, month) as month,
                -- extract(month from ga_date) month,
                unix_date(cast(date_trunc(cast(ga_date as date), month) as date)) unix_date,
                _sdc_received_at time_of_entry,
                cast(_sdc_received_at as date) date_of_entry,
                first_value(_sdc_received_at) OVER (PARTITION BY web_property_id, ga_landingpagepath, ga_hostname, date_trunc(cast(ga_date as date), month)  ORDER BY _sdc_received_at desc) lv,
                replace(ga_hostname,'www.','') domain,
                trim(replace(ga_hostname,'www.',''),'/') domain_trimmed,
                ga_landingpagepath landing_page_path,
                cast(ga_sessions as int64) sessions,
                cast(ga_transactionrevenue as int64) transaction_revenue,
                cast(ga_transactions as int64) transactions,
                cast(ga_goalcompletionsall as int64) goal_completions_all_goals,
                cast(ga_bouncerate as int64) bounces,
                cast(cast(ga_avgsessionduration as BIGNUMERIC) as int64)     seconds_on_site
                FROM     {{ source('bq_sources', 'ga') }}
                WHERE ( ga_sessions > 0 or ga_transactions > 0 or ga_goalcompletionsall > 0 )
                and ga_landingpagepath not like '%.xml%'
                and ga_landingpagepath != '(not set)'

                ) a
        LEFT JOIN {{ ref('domains_proc') }} b
        ON (
            a.account = b.google_analytics_account
        )
        WHERE time_of_entry = lv
        GROUP BY b.site, a.domain, account, month, unix_date, date_of_entry, url_untrimmed, url_trimmed
    )
)

GROUP BY site, domain, account, date, unix_date, date_of_entry, url




