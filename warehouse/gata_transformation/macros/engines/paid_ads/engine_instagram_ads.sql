{# 
  Engine: Instagram Ads → Ad Performance
  Same API as Facebook, filtered by source_platform = 'instagram_ads'
#}
{% macro engine_instagram_ads_performance(tenant_slug) %}
SELECT
    tenant_slug,
    source_platform,
    report_date,
    campaign_id,
    adset_id        AS ad_group_id,
    ad_id,
    spend,
    impressions,
    clicks,
    conversions
FROM {{ ref('int_' ~ tenant_slug ~ '__instagram_ads_facebook_insights') }}
{% endmacro %}

{% macro engine_instagram_ads_campaigns(tenant_slug) %}
SELECT
    tenant_slug,
    source_platform,
    campaign_id,
    campaign_name,
    status AS campaign_status
FROM {{ ref('int_' ~ tenant_slug ~ '__instagram_ads_campaigns') }}
{% endmacro %}
