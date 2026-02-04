{% macro engine_facebook_ads_ads(tenant_slug, source_platform='facebook_ads') %}
{#
    Engine: Facebook Ads (Ad → Creative mapping)
    Input: platform_mm__facebook_ads_api_v1_ads
    Mock data fields: id, adset_id, campaign_id, name, creative_id, status
#}
SELECT
    tenant_slug,
    raw_data_payload->>'name' as ad_name,
    raw_data_payload->>'id' as ad_id,
    raw_data_payload->>'adset_id' as adset_id,
    raw_data_payload->>'campaign_id' as campaign_id,
    raw_data_payload->>'creative_id' as creative_id
FROM {{ ref('platform_mm__facebook_ads_api_v1_ads') }}
WHERE tenant_slug = '{{ tenant_slug }}'
  AND source_platform = '{{ source_platform }}'
{% endmacro %}
