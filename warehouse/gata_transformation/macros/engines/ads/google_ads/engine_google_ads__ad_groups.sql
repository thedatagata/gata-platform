{% macro engine_google_ads_ad_groups(tenant_slug) %}
{#
    Engine: Google Ads Ad Groups (dimension lookup)
    Input: platform_mm__google_ads_api_v1_ad_groups
    Mock data fields: resource_name, id, campaign_id, name, status, type
    Note: No UTM tracking templates in mock data
#}
SELECT
    tenant_slug,
    raw_data_payload->>'id' as ad_group_id,
    raw_data_payload->>'campaign_id' as campaign_id,
    raw_data_payload->>'name' as ad_group_name,
    raw_data_payload->>'status' as ad_group_status,
    raw_data_payload->>'type' as ad_group_type,
    CAST(NULL AS VARCHAR) as utm_source,
    CAST(NULL AS VARCHAR) as utm_medium,
    CAST(NULL AS VARCHAR) as utm_campaign
FROM {{ ref('platform_mm__google_ads_api_v1_ad_groups') }}
WHERE tenant_slug = '{{ tenant_slug }}'
{% endmacro %}
