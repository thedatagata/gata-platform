{# 
  Engine: Facebook Ads → Ad Performance
  Reads: int_{tenant_slug}__facebook_ads_facebook_insights
  Normalizes to common ad performance schema
#}
{% macro engine_facebook_ads_performance(tenant_slug) %}
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
FROM {{ ref('int_' ~ tenant_slug ~ '__facebook_ads_facebook_insights') }}
{% endmacro %}

{% macro engine_facebook_ads_campaigns(tenant_slug) %}
SELECT
    tenant_slug,
    source_platform,
    campaign_id,
    campaign_name,
    status AS campaign_status
FROM {{ ref('int_' ~ tenant_slug ~ '__facebook_ads_campaigns') }}
{% endmacro %}
