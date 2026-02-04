{% macro engine_google_ads_ad_performance(tenant_slug) %}
{#
    Engine: Google Ads Ad Performance (primary metrics engine)
    Input: platform_mm__google_ads_api_v1_ad_performance (metrics fact)
    Joins: campaigns, ad_groups for dimension names
    
    Mock data fields (ad_performance): ad_id, ad_group_id, campaign_id, date, customer_id, cost_micros, impressions, clicks, conversions
    Mock data fields (campaigns): resource_name, id, name, status, advertising_channel_type
    Mock data fields (ad_groups): resource_name, id, campaign_id, name, status, type
#}
WITH performance AS (
    SELECT
        tenant_slug,
        raw_data_payload->>'ad_id' as ad_id,
        raw_data_payload->>'ad_group_id' as ad_group_id,
        raw_data_payload->>'campaign_id' as campaign_id,
        raw_data_payload->>'customer_id' as customer_id,
        CAST(raw_data_payload->>'date' AS DATE) as date,
        CAST(raw_data_payload->>'cost_micros' AS BIGINT)::DOUBLE / 1000000 as spend,
        CAST(raw_data_payload->>'impressions' AS BIGINT) as impressions,
        CAST(raw_data_payload->>'clicks' AS BIGINT) as clicks,
        CAST(raw_data_payload->>'conversions' AS DOUBLE) as conversions,
        raw_data_payload
    FROM {{ ref('platform_mm__google_ads_api_v1_ad_performance') }}
    WHERE tenant_slug = '{{ tenant_slug }}'
),

campaigns AS (
    SELECT DISTINCT
        raw_data_payload->>'id' as campaign_id,
        raw_data_payload->>'name' as campaign_name,
        raw_data_payload->>'advertising_channel_type' as channel_type
    FROM {{ ref('platform_mm__google_ads_api_v1_campaigns') }}
    WHERE tenant_slug = '{{ tenant_slug }}'
),

ad_groups AS (
    SELECT DISTINCT
        raw_data_payload->>'id' as ad_group_id,
        raw_data_payload->>'campaign_id' as campaign_id,
        raw_data_payload->>'name' as ad_group_name
    FROM {{ ref('platform_mm__google_ads_api_v1_ad_groups') }}
    WHERE tenant_slug = '{{ tenant_slug }}'
)

SELECT
    p.tenant_slug,
    p.date,
    c.campaign_name,
    g.ad_group_name,
    p.campaign_id,
    p.ad_group_id,
    p.ad_id,
    p.customer_id,
    p.spend,
    p.impressions,
    p.clicks,
    p.conversions,
    CAST(NULL AS VARCHAR) as utm_source,
    CAST(NULL AS VARCHAR) as utm_medium,
    CAST(NULL AS VARCHAR) as utm_campaign,
    p.raw_data_payload
FROM performance p
LEFT JOIN campaigns c ON p.campaign_id = c.campaign_id
LEFT JOIN ad_groups g ON p.ad_group_id = g.ad_group_id
{% endmacro %}
