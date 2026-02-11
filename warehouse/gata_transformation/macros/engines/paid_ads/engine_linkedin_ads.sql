{# 
  Engine: LinkedIn Ads → Ad Performance
  Reads: int_{tenant_slug}__linkedin_ads_ad_analytics_by_campaign
#}
{% macro engine_linkedin_ads_performance(tenant_slug) %}
SELECT
    tenant_slug,
    source_platform,
    report_date,
    campaign_id,
    CAST(NULL AS VARCHAR)   AS ad_group_id,
    CAST(NULL AS VARCHAR)   AS ad_id,
    spend,
    impressions,
    clicks,
    conversions
FROM {{ ref('int_' ~ tenant_slug ~ '__linkedin_ads_ad_analytics_by_campaign') }}
{% endmacro %}

{% macro engine_linkedin_ads_campaigns(tenant_slug) %}
SELECT
    tenant_slug,
    source_platform,
    campaign_id,
    campaign_name,
    status AS campaign_status
FROM {{ ref('int_' ~ tenant_slug ~ '__linkedin_ads_campaigns') }}
{% endmacro %}
