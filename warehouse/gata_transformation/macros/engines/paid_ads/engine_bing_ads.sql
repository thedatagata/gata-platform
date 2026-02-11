{# 
  Engine: Bing Ads → Ad Performance
  Reads: int_{tenant_slug}__bing_ads_account_performance_report
  Note: Bing's report is account-level, no ad_group_id or ad_id
#}
{% macro engine_bing_ads_performance(tenant_slug) %}
SELECT
    tenant_slug,
    source_platform,
    report_date,
    CAST(NULL AS VARCHAR)   AS campaign_id,
    CAST(NULL AS VARCHAR)   AS ad_group_id,
    CAST(NULL AS VARCHAR)   AS ad_id,
    spend,
    impressions,
    clicks,
    CAST(NULL AS DOUBLE)    AS conversions
FROM {{ ref('int_' ~ tenant_slug ~ '__bing_ads_account_performance_report') }}
{% endmacro %}
