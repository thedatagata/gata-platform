{% macro engine_facebook_ads_insights(tenant_slug, conversion_event_pattern='', source_platform='facebook_ads') %}
{#
    Engine: Facebook Ads Insights
    Input: platform_mm__facebook_ads_api_v1_facebook_insights (metrics fact)
    Joins: campaigns, ad_sets, ads master models for dimension names
    
    Mock data fields (facebook_insights): ad_id, campaign_id, adset_id, date_start, spend, impressions, clicks, conversions
    Mock data fields (campaigns): id, name, objective, status
    Mock data fields (ad_sets): id, campaign_id, name, status, daily_budget
    Mock data fields (ads): id, adset_id, campaign_id, name, creative_id, status
#}
WITH insights AS (
    SELECT
        tenant_slug,
        raw_data_payload->>'campaign_id' as campaign_id,
        raw_data_payload->>'adset_id' as adset_id,
        raw_data_payload->>'ad_id' as ad_id,
        CAST(raw_data_payload->>'date_start' AS DATE) as date,
        CAST(raw_data_payload->>'spend' AS DOUBLE) as spend,
        CAST(raw_data_payload->>'impressions' AS BIGINT) as impressions,
        CAST(raw_data_payload->>'clicks' AS BIGINT) as clicks,
        CAST(raw_data_payload->>'conversions' AS DOUBLE) as conversions,
        raw_data_payload
    FROM {{ ref('platform_mm__facebook_ads_api_v1_facebook_insights') }}
    WHERE tenant_slug = '{{ tenant_slug }}'
      AND source_platform = '{{ source_platform }}'
),

campaigns AS (
    SELECT DISTINCT
        raw_data_payload->>'id' as campaign_id,
        raw_data_payload->>'name' as campaign_name
    FROM {{ ref('platform_mm__facebook_ads_api_v1_campaigns') }}
    WHERE tenant_slug = '{{ tenant_slug }}'
      AND source_platform = '{{ source_platform }}'
),

ad_sets AS (
    SELECT DISTINCT
        raw_data_payload->>'id' as adset_id,
        raw_data_payload->>'name' as adset_name
    FROM {{ ref('platform_mm__facebook_ads_api_v1_ad_sets') }}
    WHERE tenant_slug = '{{ tenant_slug }}'
      AND source_platform = '{{ source_platform }}'
),

ads AS (
    SELECT DISTINCT
        raw_data_payload->>'id' as ad_id,
        raw_data_payload->>'name' as ad_name
    FROM {{ ref('platform_mm__facebook_ads_api_v1_ads') }}
    WHERE tenant_slug = '{{ tenant_slug }}'
      AND source_platform = '{{ source_platform }}'
)

SELECT
    i.tenant_slug,
    i.date,
    c.campaign_name,
    s.adset_name,
    a.ad_name,
    i.campaign_id,
    i.adset_id,
    i.ad_id,
    i.spend,
    i.impressions,
    i.clicks,
    {% if conversion_event_pattern %}
    {# Config-driven conversion filtering (for sources with JSON conversion arrays) #}
    COALESCE(i.conversions, 0)
    {% else %}
    COALESCE(i.conversions, 0)
    {% endif %} AS conversions,
    i.raw_data_payload
FROM insights i
LEFT JOIN campaigns c ON i.campaign_id = c.campaign_id
LEFT JOIN ad_sets s ON i.adset_id = s.adset_id
LEFT JOIN ads a ON i.ad_id = a.ad_id
WHERE i.spend > 0
{% endmacro %}
