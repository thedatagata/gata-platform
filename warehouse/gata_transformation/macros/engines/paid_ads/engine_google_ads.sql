{# 
  Engine: Google Ads → Ad Performance
  Reads: int_{tenant_slug}__google_ads_ad_performance
#}
{% macro engine_google_ads_performance(tenant_slug) %}
SELECT
    tenant_slug,
    source_platform,
    report_date,
    campaign_id,
    ad_group_id,
    ad_id,
    spend,
    impressions,
    clicks,
    conversions
FROM {{ ref('int_' ~ tenant_slug ~ '__google_ads_ad_performance') }}
{% endmacro %}

{% macro engine_google_ads_campaigns(tenant_slug) %}
SELECT
    tenant_slug,
    source_platform,
    campaign_id,
    campaign_name,
    status AS campaign_status
FROM {{ ref('int_' ~ tenant_slug ~ '__google_ads_campaigns') }}
{% endmacro %}
