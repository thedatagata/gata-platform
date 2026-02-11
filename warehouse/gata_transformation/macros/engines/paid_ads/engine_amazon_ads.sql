{# 
  Engine: Amazon Ads → Ad Performance
  Reads: int_{tenant_slug}__amazon_ads_sponsored_products_product_ads
#}
{% macro engine_amazon_ads_performance(tenant_slug) %}
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
FROM {{ ref('int_' ~ tenant_slug ~ '__amazon_ads_sponsored_products_product_ads') }}
{% endmacro %}

{% macro engine_amazon_ads_campaigns(tenant_slug) %}
SELECT
    tenant_slug,
    source_platform,
    campaign_id,
    campaign_name,
    status AS campaign_status
FROM {{ ref('int_' ~ tenant_slug ~ '__amazon_ads_campaigns') }}
{% endmacro %}
