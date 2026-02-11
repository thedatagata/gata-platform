{# 
  Engine: TikTok Ads → Ad Performance
  Reads: int_{tenant_slug}__tiktok_ads_ads_reports_daily
#}
{% macro engine_tiktok_ads_performance(tenant_slug) %}
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
FROM {{ ref('int_' ~ tenant_slug ~ '__tiktok_ads_ads_reports_daily') }}
{% endmacro %}
