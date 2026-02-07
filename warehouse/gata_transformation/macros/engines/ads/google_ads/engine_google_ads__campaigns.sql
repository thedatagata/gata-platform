{% macro engine_google_ads_campaign(tenant_slug) %}
{#
    Engine: Google Ads Campaigns (dimension lookup)
    Input: platform_mm__google_ads_api_v1_campaigns
    Mock data fields: resource_name, id, name, status, advertising_channel_type
    Note: No metrics in campaigns table - use engine_google_ads_ad_performance for metrics
#}
SELECT
    tenant_slug,
    raw_data_payload->>'id' as campaign_id,
    raw_data_payload->>'name' as campaign_name,
    raw_data_payload->>'status' as campaign_status,
    raw_data_payload->>'advertising_channel_type' as channel_type,
    raw_data_payload->>'resource_name' as resource_name
FROM {{ ref('platform_mm__google_ads_api_v1_campaigns') }}
WHERE tenant_slug = '{{ tenant_slug }}'
{% endmacro %}
