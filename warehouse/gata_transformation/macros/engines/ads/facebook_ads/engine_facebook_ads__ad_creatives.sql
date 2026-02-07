{% macro engine_facebook_ads_creatives(tenant_slug, source_platform='facebook_ads') %}
{#
    Engine: Facebook Ad Creatives (UTM extraction)
    Input: platform_mm__facebook_ads_api_v1_ads
    Note: Mock data does not include a separate ad_creatives table.
          Creative data is embedded in the ads table via creative_id.
          This engine extracts what's available from the ads dimension.
#}
SELECT
    tenant_slug,
    raw_data_payload->>'creative_id' as creative_id,
    raw_data_payload->>'name' as ad_name,
    CAST(NULL AS VARCHAR) as url_tags,
    CAST(NULL AS VARCHAR) as utm_source,
    CAST(NULL AS VARCHAR) as utm_medium,
    CAST(NULL AS VARCHAR) as utm_campaign
FROM {{ ref('platform_mm__facebook_ads_api_v1_ads') }}
WHERE tenant_slug = '{{ tenant_slug }}'
  AND source_platform = '{{ source_platform }}'
{% endmacro %}
